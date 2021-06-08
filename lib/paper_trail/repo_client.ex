defmodule PaperTrail.RepoClient do
  @doc """
  Gets the configured repo module or defaults to Repo if none configured
  """
  @spec repo(PaperTrail.options()) :: PaperTrail.repo()
  def repo(options \\ []) do
    case Keyword.get(options, :repo) do
      nil -> Application.get_env(:paper_trail, :repo, Repo)
      repo -> repo
    end
  end

  @spec originator :: PaperTrail.originator()
  def originator, do: Application.get_env(:paper_trail, :originator, nil)

  @spec originator_type :: atom()
  def originator_type, do: Application.get_env(:paper_trail, :originator_type, :integer)

  @spec strict_mode(PaperTrail.options()) :: PaperTrail.strict_mode()
  def strict_mode(options \\ []) do
    case Keyword.get(options, :strict_mode) do
      nil -> Application.get_env(:paper_trail, :strict_mode, false)
      strict_mode -> strict_mode
    end
  end

  @spec return_operation(PaperTrail.options()) :: PaperTrail.multi_name()
  def return_operation(options \\ []) do
    case Keyword.fetch(options, :return_operation) do
      :error -> Application.get_env(:paper_trail, :return_operation, nil)
      {:ok, return_operation} -> return_operation
    end
  end
end
