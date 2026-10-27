# Shared setup for the R notebooks that use synapser.
#
# Usage, from a notebook's setup chunk (path relative to the notebook):
#   source("../../setup_synapser.R")
#   setup_synapser()
#   library(synapser)
#
# synapser 2.1.5.356 is hard-pinned to Python synapseclient==4.4.2 (see its
# R/zzz.R), which conflicts with the newer synapseclient the ADT package itself
# requires. This function points reticulate at a venv dedicated to these R
# notebooks, created and managed by reticulate itself, so the two never collide.
#
# Call it before anything initializes reticulate's Python in the session.
setup_synapser <- function(version = "2.1.5.356",
                           venv = "agora-data-tools-r-synapser") {
  # A VIRTUAL_ENV left over from an unrelated project's activated shell silently
  # overrides reticulate's env selection. Clearing
  # VIRTUAL_ENV and exporting RETICULATE_PYTHON as a real environment variable
  # (not just an R-level call) makes this robust regardless of shell state, and
  # ensures that child process picks up the right Python too.
  Sys.unsetenv("VIRTUAL_ENV")

  # Pinned to Python 3.11 (matching this project's own setup.cfg)
  library(reticulate)
  reticulate::virtualenv_create(venv, version = "3.11")
  Sys.setenv(RETICULATE_PYTHON = file.path(
    reticulate::virtualenv_root(), venv, "bin", "python"
  ))
  reticulate::use_virtualenv(venv, required = TRUE)

  if (!require("remotes", quietly = TRUE)) {
    install.packages("remotes")
  }

  install_pinned_package("synapser", version)
}

# Installs `pkg` at exactly `version` unless that version is already installed.
#
# The installed version is read from the package's DESCRIPTION file instead of
# loading the package. Loading it first (e.g. with require()) and then
# reinstalling a different version over it corrupts the package's lazy-load
# database ("lazy-load database ... is corrupt"). If the package is already
# loaded in this session, the R session has to be restarted before its version
# can change.
install_pinned_package <- function(pkg, version,
                                   repos = c("http://ran.synapse.org",
                                             "https://cloud.r-project.org")) {
  installed <- tryCatch(as.character(packageVersion(pkg)),
                        error = function(e) NA_character_)
  if (!is.na(installed) && installed == version) {
    return(invisible(FALSE))
  }
  if (pkg %in% loadedNamespaces()) {
    stop(sprintf(
      "%s %s is already loaded in this R session but %s is required. Restart R, then run this again.",
      pkg, installed, version
    ), call. = FALSE)
  }
  remotes::install_version(pkg, version = version, repos = repos)
  invisible(TRUE)
}
