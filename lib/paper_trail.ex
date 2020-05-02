defmodule PaperTrail do
  import Ecto.Changeset

  alias PaperTrail.Multi
  alias PaperTrail.RepoClient
  alias PaperTrail.Serializer
  alias PaperTrail.Version
  alias PaperTrail.VersionQueries

  @type opts :: [
    repo: module | nil,
    strict_mode: boolean | nil,
    origin: String.t() | nil,
    meta: map | nil,
    originator: Ecto.Schema.t() | nil,
    prefix: String.t() | nil
  ]

  @type insert_opts :: [
    repo: module | nil,
    strict_mode: boolean | nil,
    origin: String.t() | nil,
    meta: map | nil,
    originator: Ecto.Schema.t() | nil,
    prefix: String.t() | nil,
    model_key: atom,
    version_key: atom
  ]

  @callback insert(Ecto.Changeset.t(), insert_opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback insert!(Ecto.Changeset.t(), insert_opts) :: Ecto.Schema.t()
  @callback update(Ecto.Changeset.t(), opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback update!(Ecto.Changeset.t(), opts) :: Ecto.Schema.t()
  @callback delete(Ecto.Changeset.t(), opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback delete!(Ecto.Changeset.t(), opts) :: Ecto.Schema.t()

  @callback get_version(Ecto.Schema.t()) :: Ecto.Query.t()
  @callback get_version(Ecto.Schema.t(), any) :: Ecto.Query.t()
  @callback get_version(Ecto.Schema.t(), any, keyword) :: Ecto.Query.t()

  @callback get_versions(Ecto.Schema.t()) :: Ecto.Query.t()
  @callback get_versions(Ecto.Schema.t(), any) :: Ecto.Query.t()
  @callback get_versions(Ecto.Schema.t(), any, keyword) :: Ecto.Query.t()

  @callback get_current_model(Version.t()) :: Ecto.Schema.t()

  defmacro __using__(opts \\ []) do
    client_opts = [
      repo: RepoClient.repo(opts),
      strict_mode: RepoClient.strict_mode(opts)
    ]

    quote do
      @behaviour PaperTrail

      @impl true
      def insert(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, model_key: :model, version_key: :version]) do
        PaperTrail.insert(changeset, merge_options(options))
      end

      @impl true
      def insert!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, model_key: :model, version_key: :version]) do
        PaperTrail.insert!(changeset, merge_options(options))
      end

      @impl true
      def update(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
        PaperTrail.update(changeset, merge_options(options))
      end

      @impl true
      def update!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
        PaperTrail.update!(changeset, merge_options(options))
      end

      @impl true
      def delete(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
        PaperTrail.delete(struct, merge_options(options))
      end

      @impl true
      def delete!(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
        PaperTrail.delete!(struct, merge_options(options))
      end

      @impl true
      def get_version(record) do
        VersionQueries.get_version(record, unquote(client_opts))
      end

      @impl true
      def get_version(model_or_record, options) when is_list(options) do
        VersionQueries.get_version(model_or_record, merge_options(options))
      end

      @impl true
      def get_version(model_or_record, id) do
        VersionQueries.get_version(model_or_record, id, unquote(client_opts))
      end

      @impl true
      def get_version(model, id, options) do
        VersionQueries.get_version(model, id, merge_options(options))
      end

      @impl true
      def get_versions(record) do
        VersionQueries.get_versions(record, unquote(client_opts))
      end

      @impl true
      def get_versions(model_or_record, options) when is_list(options) do
        VersionQueries.get_versions(model_or_record, merge_options(options))
      end

      @impl true
      def get_versions(model_or_record, id) do
        VersionQueries.get_versions(model_or_record, id, unquote(client_opts))
      end

      @impl true
      def get_versions(model, id, options) do
        VersionQueries.get_versions(model, id, merge_options(options))
      end

      @impl true
      def get_current_model(version) do
        VersionQueries.get_current_model(version, unquote(client_opts))
      end

      @spec merge_options(keyword) :: keyword
      defp merge_options(options), do: Keyword.merge(options, unquote(client_opts))
    end
  end

  defdelegate get_version(record), to: VersionQueries
  defdelegate get_version(model_or_record, id_or_options), to: VersionQueries
  defdelegate get_version(model, id, options), to: VersionQueries
  defdelegate get_versions(record), to: VersionQueries
  defdelegate get_versions(model_or_record, id_or_options), to: VersionQueries
  defdelegate get_versions(model, id, options), to: VersionQueries
  defdelegate get_current_model(version, options \\ []), to: VersionQueries
  defdelegate make_version_struct(version, model, options), to: Serializer
  defdelegate get_sequence_from_model(changeset, options \\ []), to: Serializer
  defdelegate serialize(data), to: Serializer
  defdelegate get_sequence_id(table_name, options \\ []), to: Serializer
  defdelegate add_prefix(changeset, prefix), to: Serializer
  defdelegate get_item_type(data), to: Serializer
  defdelegate get_model_id(model), to: Serializer

  @doc """
  Inserts a record to the database with a related version insertion in one transaction
  """
  @spec insert(Ecto.Changeset.t(), insert_opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def insert(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, model_key: :model, version_key: :version]) do
    Multi.new()
    |> Multi.insert(changeset, options)
    |> Multi.commit(options)
  end

  @doc """
  Same as insert/2 but returns only the model struct or raises if the changeset is invalid.
  """
  @spec insert!(Ecto.Schema.t(), insert_opts) :: Ecto.Schema.t()
  def insert!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, model_key: :model, version_key: :version]) do
    repo = RepoClient.repo(options)

    repo.transaction(fn ->
      case RepoClient.strict_mode(options) do
        true ->
          version_id = get_sequence_id("versions", options) + 1

          changeset_data =
            Map.get(changeset, :data, changeset)
            |> Map.merge(%{
              id: get_sequence_from_model(changeset, options) + 1,
              first_version_id: version_id,
              current_version_id: version_id
            })

          initial_version =
            make_version_struct(%{event: "insert"}, changeset_data, options)
            |> repo.insert!

          updated_changeset =
            changeset
            |> change(%{
              first_version_id: initial_version.id,
              current_version_id: initial_version.id
            })

          model = repo.insert!(updated_changeset)
          target_version = make_version_struct(%{event: "insert"}, model, options) |> serialize()
          Version.changeset(initial_version, target_version) |> repo.update!
          model

        _ ->
          model = repo.insert!(changeset)
          make_version_struct(%{event: "insert"}, model, options) |> repo.insert!
          model
      end
    end)
    |> elem(1)
  end

  @doc """
  Updates a record from the database with a related version insertion in one transaction
  """
  @spec update(Ecto.Changeset.t(), opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
    Multi.new()
    |> Multi.update(changeset, options)
    |> Multi.commit(options)
  end

  @doc """
  Same as update/2 but returns only the model struct or raises if the changeset is invalid.
  """
  @spec update!(Ecto.Schema.t(), opts) :: Ecto.Schema.t()
  def update!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
    repo = RepoClient.repo(options)

    repo.transaction(fn ->
      case RepoClient.strict_mode(options) do
        true ->
          version_data =
            changeset.data
            |> Map.merge(%{
              current_version_id: get_sequence_id("versions", options)
            })

          target_changeset = changeset |> Map.merge(%{data: version_data})
          target_version = make_version_struct(%{event: "update"}, target_changeset, options)
          initial_version = repo.insert!(target_version)
          updated_changeset = changeset |> change(%{current_version_id: initial_version.id})
          model = repo.update!(updated_changeset)

          new_item_changes =
            initial_version.item_changes
            |> Map.merge(%{
              current_version_id: initial_version.id
            })

          initial_version |> change(%{item_changes: new_item_changes}) |> repo.update!
          model

        _ ->
          model = repo.update!(changeset)
          version_struct = make_version_struct(%{event: "update"}, changeset, options)
          repo.insert!(version_struct)
          model
      end
    end)
    |> elem(1)
  end

  @doc """
  Deletes a record from the database with a related version insertion in one transaction
  """
  @spec delete(Ecto.Changeset.t(), opts) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def delete(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
    Multi.new()
    |> Multi.delete(struct, options)
    |> Multi.commit(options)
  end

  @doc """
  Same as delete/2 but returns only the model struct or raises if the changeset is invalid.
  """
  @spec delete!(Ecto.Schema.t(), opts) :: Ecto.Schema.t()
  def delete!(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil]) do
    repo = RepoClient.repo(options)

    repo.transaction(fn ->
      model = repo.delete!(struct, options)
      version_struct = make_version_struct(%{event: "delete"}, struct, options)
      repo.insert!(version_struct, options)
      model
    end)
    |> elem(1)
  end
end
