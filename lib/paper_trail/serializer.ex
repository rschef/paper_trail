defmodule PaperTrail.Serializer do
  import Ecto.Query

  alias PaperTrail.RepoClient
  alias PaperTrail.Version

  def make_version_struct(%{event: "insert"}, model, options) do
    originator = RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "insert",
      item_type: get_item_type(model),
      item_id: get_model_id(model),
      item_changes: serialize(model),
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
      item_changes: serialize_changes(changeset),
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
      item_changes: serialize(model_or_changeset),
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
          event: "update",
          item_type: ^item_type,
          item_id: q.id,
          item_changes: type(^changes_map, :map),
          originator_id: ^originator_id,
          origin: ^origin,
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

  def serialize(nil), do: nil
  def serialize(%Ecto.Changeset{data: data}), do: serialize(data)
  def serialize(%_schema{} = model), do: Ecto.embedded_dump(model, :json)

  def serialize_changes(%Ecto.Changeset{data: %schema{}, changes: changes}) do
    changes
    |> schema.__struct__()
    |> serialize()
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
end
