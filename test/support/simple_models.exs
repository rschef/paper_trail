defmodule LocationType do
  use Ecto.Type

  defstruct [:country]

  @impl true
  def type, do: :map

  @impl true
  def cast(%__MODULE__{} = location), do: {:ok, location}
  def cast(%{} = data), do: {:ok, struct!(__MODULE__, data)}
  def cast(_), do: :error

  @impl true
  def load(data) when is_map(data) do
    data = Enum.map(data, fn {key, val} -> {String.to_existing_atom(key), val} end)

    {:ok, struct!(__MODULE__, data)}
  end

  @impl true
  def dump(%__MODULE__{} = location), do: {:ok, Map.from_struct(location)}
  def dump(_), do: :error
end

defmodule EmailOptions do
  use Ecto.Schema

  @primary_key false
  embedded_schema do
    field(:newsletter_enabled, :boolean)
  end

  def changeset(options, params) do
    Ecto.Changeset.cast(options, params, [:newsletter_enabled])
  end
end

defmodule SimpleCompany do
  use Ecto.Schema

  alias PaperTrailTest.MultiTenantHelper, as: MultiTenant

  import Ecto.Changeset
  import Ecto.Query

  schema "simple_companies" do
    field(:name, :string)
    field(:is_active, :boolean)
    field(:website, :string)
    field(:city, :string)
    field(:address, :string)
    field(:facebook, :string)
    field(:twitter, :string)
    field(:founded_in, :string)
    field(:location, LocationType)
    embeds_one(:email_options, EmailOptions, on_replace: :update)

    has_many(:people, SimplePerson, foreign_key: :company_id)

    timestamps()
  end

  @optional_fields ~w(
    name
    is_active
    website
    city
    address
    facebook
    twitter
    founded_in
    location
  )a

  def changeset(model, params \\ %{}) do
    model
    |> cast(params, @optional_fields)
    |> cast_embed(:email_options, with: &EmailOptions.changeset/2)
    |> validate_required([:name])
    |> no_assoc_constraint(:people)
  end

  def count do
    from(record in __MODULE__, select: count(record.id)) |> PaperTrail.RepoClient.repo().one
  end

  def count(:multitenant) do
    from(record in __MODULE__, select: count(record.id))
    |> MultiTenant.add_prefix_to_query()
    |> PaperTrail.RepoClient.repo().one
  end
end

defmodule SimplePerson do
  use Ecto.Schema

  alias PaperTrailTest.MultiTenantHelper, as: MultiTenant

  import Ecto.Changeset
  import Ecto.Query

  schema "simple_people" do
    field(:first_name, :string)
    field(:last_name, :string)
    field(:visit_count, :integer)
    field(:gender, :boolean)
    field(:birthdate, :date)

    belongs_to(:company, SimpleCompany, foreign_key: :company_id, on_replace: :update)

    timestamps()
  end

  @optional_fields ~w(
    first_name
    last_name
    visit_count
    gender
    birthdate
    company_id
  )a

  def changeset(model, params \\ %{}) do
    model
    |> cast(params, @optional_fields)
    |> foreign_key_constraint(:company_id)
  end

  def with_company_changeset(model, params) do
    model
    |> cast(params, @optional_fields)
    |> cast_assoc(:company, with: &SimpleCompany.changeset/2)
  end

  def count do
    from(record in __MODULE__, select: count(record.id)) |> PaperTrail.RepoClient.repo().one
  end

  def count(:multitenant) do
    from(record in __MODULE__, select: count(record.id))
    |> MultiTenant.add_prefix_to_query()
    |> PaperTrail.RepoClient.repo().one
  end
end
