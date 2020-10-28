defmodule PaperTrail.Serializer do
  import Ecto.Query

  alias PaperTrail.RepoClient
  alias PaperTrail.Version

  @type options :: PaperTrail.options()

  @default_ignored_ecto_types [Ecto.UUID, :binary_id, :binary]

  def make_version_struct(%{event: "insert"}, model, options) do
    originator = RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "insert",
      item_type: get_item_type(model),
      item_id: get_model_id(model),
      item_changes: serialize(model, options),
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  def make_version_struct(%{event: "update"}, changeset, options) do
    originator = RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "update",
      item_type: get_item_type(changeset),
      item_id: get_model_id(changeset),
      item_changes: serialize(changeset, options, "update"),
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  def make_version_struct(%{event: "delete"}, model_or_changeset, options) do
    originator = RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "delete",
      item_type: get_item_type(model_or_changeset),
      item_id: get_model_id(model_or_changeset),
      item_changes: serialize(model_or_changeset, options),
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  @spec make_version_structs(map, PaperTrail.queryable(), Keyword.t() | map, PaperTrail.options()) ::
          [map]
  def make_version_structs(%{event: "update"}, queryable, changes, options) do
    {_table, schema} = queryable.from.source
    item_type = schema |> struct() |> get_item_type()
    [primary_key] = schema.__schema__(:primary_key)
    changes_map = Map.new(changes)
    originator = RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]
    originator_id = if(originator_ref, do: originator_ref.id, else: nil)
    origin = options[:origin]
    meta = options[:meta]
    repo = RepoClient.repo(options)

    repo.all(
      from(q in queryable,
        select: %{
          event: type(^"update", :string),
          item_type: type(^item_type, :string),
          item_id: field(q, ^primary_key),
          item_changes: type(^changes_map, :map),
          originator_id: type(^originator_id, :string),
          origin: type(^origin, :string),
          meta: type(^meta, :map),
          inserted_at: type(fragment("CURRENT_TIMESTAMP"), :naive_datetime)
        }
      )
    )
  end

  def get_sequence_from_model(changeset, options \\ []) do
    table_name =
      case Map.get(changeset, :data) do
        nil -> changeset.__struct__.__schema__(:source)
        _ -> changeset.data.__struct__.__schema__(:source)
      end

    get_sequence_id(table_name, options)
  end

  def get_sequence_id(table_name, options) do
    Ecto.Adapters.SQL.query!(
      RepoClient.repo(options),
      "select last_value FROM #{table_name}_id_seq"
    ).rows
    |> List.first()
    |> List.first()
  end

  @spec serialize(
          nil | Ecto.Changeset.t() | struct() | [Ecto.Changeset.t() | struct()],
          options(),
          String.t()
        ) :: nil | map() | [map()]
  def serialize(model, options, event \\ "insert")

  def serialize(nil, _options, _event), do: nil

  def serialize(list, options, event) when is_list(list) do
    Enum.map(list, &serialize(&1, options, event))
  end

  def serialize(
        %Ecto.Changeset{data: %schema{}, changes: changes},
        options,
        "update"
      ) do
    changes
    |> schema.__struct__()
    |> do_serialize(options, "update", Map.keys(changes))
  end

  def serialize(%Ecto.Changeset{data: data}, options, event) do
    do_serialize(data, options, event)
  end

  def serialize(%_schema{} = model, options, event), do: do_serialize(model, options, event)

  @spec do_serialize(struct, options, String.t(), [atom] | nil) :: map
  def do_serialize(%schema{} = model, options, event, changed_fields \\ nil) do
    fields = changed_fields || schema.__schema__(:fields)
    repo = RepoClient.repo(options)
    {adapter, _adapter_meta} = Ecto.Repo.Registry.lookup(repo.get_dynamic_repo())
    changes = model |> Map.from_struct() |> Map.take(fields)
    associations = serialize_associations(model, options, event)

    changes
    |> Enum.map(&dump_field!(&1, schema, adapter, options, event))
    |> Map.new()
    |> Map.merge(associations)
  end

  @spec serialize_associations(struct, options, String.t()) :: map
  defp serialize_associations(%schema{} = model, options, event) do
    association_fields = schema.__schema__(:associations)

    model
    |> Map.take(association_fields)
    |> Enum.filter(fn {_field, value} -> Ecto.assoc_loaded?(value) end)
    |> Enum.map(fn {field, value} -> {field, serialize(value, options, event)} end)
    |> Map.new()
  end

  @spec dump_field!({atom, any}, module, module, options, String.t()) :: {atom, any}
  defp dump_field!({field, %Ecto.Changeset{} = value}, _schema, _adapter, options, event) do
    {field, serialize(value, options, event)}
  end

  defp dump_field!({field, value}, schema, adapter, _options, _event) do
    dumper = schema.__schema__(:dump)
    {alias, type} = Map.fetch!(dumper, field)

    dumped_value =
      if(
        type in ignored_ecto_types(),
        do: value,
        else: do_dump_field!(schema, field, type, value, adapter)
      )

    {alias, dumped_value}
  end

  @spec do_dump_field!(module, atom, atom, any, module) :: any
  defp do_dump_field!(schema, field, type, value, adapter) do
    case Ecto.Type.adapter_dump(adapter, type, value) do
      {:ok, value} ->
        value

      :error ->
        raise Ecto.ChangeError,
              "value `#{inspect(value)}` for `#{inspect(schema)}.#{field}` " <>
                "does not match type #{inspect(type)}"
    end
  end

  def add_prefix(changeset, nil), do: changeset
  def add_prefix(changeset, prefix), do: Ecto.put_meta(changeset, prefix: prefix)

  def get_item_type(%Ecto.Changeset{data: data}), do: get_item_type(data)
  def get_item_type(%schema{}), do: schema |> Module.split() |> List.last()

  def get_model_id(%Ecto.Changeset{data: data}), do: get_model_id(data)

  def get_model_id(model) do
    {_, model_id} = List.first(Ecto.primary_key(model))

    case PaperTrail.Version.__schema__(:type, :item_id) do
      :integer ->
        model_id

      _ ->
        "#{model_id}"
    end
  end

  @spec ignored_ecto_types :: [atom]
  defp ignored_ecto_types do
    :not_dumped_ecto_types
    |> get_env([])
    |> Kernel.++(@default_ignored_ecto_types)
    |> Enum.uniq()
  end

  @spec get_env(atom, any) :: any
  defp get_env(key, default), do: Application.get_env(:paper_trail, key, default)
end
