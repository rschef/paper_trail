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
      item_changes: serialize_changes(changeset, options),
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

  @spec serialize(nil | Ecto.Changeset.t() | struct, options) :: nil | map
  def serialize(nil, _options), do: nil

  def serialize(%Ecto.Changeset{data: data}, options), do: serialize(data, options)

  def serialize(%schema{} = model, options) do
    dumper = schema.__schema__(:dump)
    fields = schema.__schema__(:fields)
    repo = RepoClient.repo(options)
    {adapter, _adapter_meta} = Ecto.Repo.Registry.lookup(repo.get_dynamic_repo())
    changes = model |> Map.from_struct() |> Map.take(fields)

    schema
    |> dump_fields!(changes, dumper, adapter)
    |> Map.new()
  end

  @spec dump_fields!(module, map, module, module) :: Keyword.t()
  defp dump_fields!(schema, changes, dumper, adapter) do
    for {field, value} <- changes do
      {alias, type} = Map.fetch!(dumper, field)

      dumped_value =
        if(
          type in ignored_ecto_types(),
          do: value,
          else: dump_field!(schema, field, type, value, adapter)
        )

      {alias, dumped_value}
    end
  end

  @spec dump_field!(module, atom, atom, any, module) :: any
  defp dump_field!(schema, field, type, value, adapter) do
    case Ecto.Type.adapter_dump(adapter, type, value) do
      {:ok, value} ->
        value

      :error ->
        raise Ecto.ChangeError,
              "value `#{inspect(value)}` for `#{inspect(schema)}.#{field}` " <>
                "does not match type #{inspect(type)}"
    end
  end

  def serialize_changes(%Ecto.Changeset{data: %schema{}, changes: changes}, options) do
    changes
    |> schema.__struct__()
    |> serialize(options)
    |> Map.take(Map.keys(changes))
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
