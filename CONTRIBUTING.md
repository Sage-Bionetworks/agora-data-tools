
## Contributing

We welcome all contributions!  That said, this is a Sage Bionetworks owned project, and we use JIRA ([AG](https://sagebionetworks.jira.com/jira/software/c/projects/AG/boards/91)/[IBCDPE](https://sagebionetworks.jira.com/jira/software/c/projects/IBCDPE/boards/189)) to track any bug/feature requests. This guide will be more focussed on a Sage Bio employee's development workflow.  If you are a Sage Bio employee, make sure to assign yourself the JIRA ticket if you decide to work on it.

## Coding Style

The code in this package is also automatically formatted by `black` for consistency.

## The Development Life Cycle

<!--
### Fork and clone this repository

1. See the [Github docs](https://help.github.com/articles/fork-a-repo/) for how to make a copy (a fork) of a repository to your own Github account.
1. Then, [clone the repository](https://help.github.com/articles/cloning-a-repository/) to your local machine so you can begin making changes.
1. Add this repository as an [upstream remote](https://help.github.com/en/articles/configuring-a-remote-for-a-fork) on your local git repository so that you are able to fetch the latest commits.
1. On your local machine make sure you have the latest version of the `develop` branch:

    ```shell
    git checkout develop
    git pull upstream develop
    ``` -->

### Install development dependencies

Please follow the [README.md](README.md) to install the package for development purposes.

### Developing at Sage Bio

The agora-data-tools project follows the standard [trunk based development](https://trunkbaseddevelopment.com/) development strategy.
> To ensure the most fluid development, do not push to your `dev`

1. Please ask for write permissions to contribute directly to this repository.
1. Make sure you are always creating feature branches from the `dev` branch.

    ```shell
    git checkout dev
    git pull
    ```

1. Create a feature branch from the `dev` branch. Use the Id of the JIRA issue that you are addressing and name the branch after the issue with some more detail like `{user}/{JIRA}-123/add-some-new-feature`.

    ```shell
    git checkout dev
    git checkout -b tyu/JIRA-123/some-new-feature
    ```

1. At this point, you have only created the branch locally, you need to push this to your fork on GitHub.

    ```shell
    git push --set-upstream origin tyu/JIRA-123/some-new-feature
    ```

    You should now be able to see the branch on GitHub. Make commits as you deem necessary. It helps to provide useful commit messages - a commit message saying 'Update' is a lot less helpful than saying 'Remove X parameter because it was unused'.

    ```shell
    git commit changed_file.txt -m "Remove X parameter because it was unused"
    git push
    ```

1. (Make sure you have follow instructions in "Install development dependencies") Once you have made your additions or changes, make sure you write tests and run the test suite.  More information on testing below.

    ```shell
    pytest -vs tests/
    ```

1. (Make sure you have follow instructions in "Install development dependencies") Make sure to run the auto python code formatter, black.

    ```shell
    black ./
    ```

1. Once you have completed all the steps above, create a pull request from the feature branch to the `dev` branch of the Sage-Bionetworks/agora-data-tools repo.

> *A code maintainer must review and accept your pull request.* A code review ideally happens with both the contributor and the reviewer present, but is not strictly required for contributing. This can be performed remotely (e.g., Zoom, Hangout, or other video or phone conference).  Most code reviews are done asyncronously.

This package uses [semantic versioning](https://semver.org/) for releasing new versions. A github release should occur at least once a quarter to capture the changes between releases.

<!-- This package uses [semantic versioning](https://semver.org/) for releasing new versions. The version should be updated on the `dev` branch as changes are reviewed and merged in by a code maintainer. The version for the package is maintained in the [agoradatatools/__init__.py](agoradatatools/__init__.py) file.  A github release should also occur every time `dev` is pushed into `main` and it should match the version for the package. -->

### Testing

#### Running tests

This package uses [`pytest`](https://pytest.org/en/latest/) to run tests. The test code is located in the [tests](./tests) subdirectory.

Here's how to run the test suite:

```shell
pytest -vs tests/
```

Tests are also run automatically by Github Actions on any pull request and are required to pass before merging.

#### Test Development

Please add tests for new code. These might include unit tests (to test specific functionality of code that was added to support fixing the bug or feature), integration tests (to test that the feature is usable - e.g., it should have complete the expected behavior as reported in the feature request or bug report), or both.

##### Mock Testing

It is recommended to use the following style (see example below) for mock testing across this package:

```python
from unittest.mock import patch
...
patch.object(MODULE_NAME, "FUNCTION_TO_MOCK_NAME".return_value=SOME_RETURN_VALUE)
```

<!-- ### Release Procedure (For Package Maintainers)

Follow gitflow best practices as linked above.

1. Always merge all new features into `develop` branch first (unless it is a documentation, readme, or github action patch into `main`)
1. After initial features are ready in the `develop` branch, create a `release-X.X` branch to prepare for the release.
    1. update the `__version__` parameter in `genie/__init__.py`
1. Merge `release-X.X` branch into `main` - Not by pull request!
1. Create release tag (`v...`) and include release notes.  Also include any known bugs for each release here.
1. Merge `main` back into `develop` -->

### Transforms

This package has a `src/agoradatatools/etl/transform` submodule.  This folder houses all the individual transform modules required for the package.  Here are the steps to add more transforms:

1. Create new script in the transform submodule that matches the dataset name and name the function `transform_...`.  For example, if you have a dataset named `genome_variants`, your new script would be `src/agoradatatools/etl/transform/transform_genome_variants.py`.
1. Register the new transform function in `src/agoradatatools/etl/transform/__init__.py`. Look in that file for examples.
1. Modify the `apply_custom_transformations` in `src/agoradatatools/process.py` to include your new transform.
1. Write a test for the transform:
    - For transform tests, we are using a [Data-Driven Testing](https://www.develer.com/en/blog/data-driven-testing-with-python/) strategy
    - To contribute new tests, assets in the form of input and output data files are needed.
        - The input file is loaded to serve as the data fed into the transform function, while the output file is loaded in to check the funciton output against.
    - These tests should include multiple ways of evaluating the transform function, including one test that should pass (good input data) and at least one that should fail (bad input data).
    - For some functions, it may be appropriate to include multiple passing datasets (e.g. for functions that are written to handle imperfect data) and/or multiple failing datasets (e.g. for transforms operating on datasets that can be unclean in multiple distinct ways).
    - Each transform function should have its own folder in `test_assets` to hold its input and output data files. Inputs should be in CSV form and outputs in JSON form.
    - Use `pytest.mark.parameterize` to loop through multiple datasets in a single test.
    - The class `TestTransformGenesBiodomains` can be used as an example for future tests contibuted.

### DockerHub

This repository does not use github actions to push docker images.  By adding the `sagebiodockerhub` github user as an Admin to this GitHub repository, we can configure an automated build in DockerHub.  You can view the repo [here](https://hub.docker.com/r/sagebionetworks/agora-data-tools).
