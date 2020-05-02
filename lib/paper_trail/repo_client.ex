defmodule PaperTrail.RepoClient do
  @doc """
  Gets the configured repo module or defaults to Repo if none configured
  """
  def repo(opts \\ []) do
    case Keyword.get(opts, :repo) do
      nil -> Application.get_env(:paper_trail, :repo, Repo)
      repo -> repo
    end
  end

  def originator, do: Application.get_env(:paper_trail, :originator, nil)

  def strict_mode(opts \\ []) do
    case Keyword.get(opts, :strict_mode) do
      nil -> Application.get_env(:paper_trail, :strict_mode, false)
      strict_mode -> strict_mode
    end
  end
end
