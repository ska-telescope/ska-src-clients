# SKA SRC Clients

Command line utilities to interface with SRCNet APIs.

[TOC]

## Install

```bash
$ python3 -m pip install ska-src-clients --index-url https://gitlab.com/api/v4/groups/70683489/-/packages/pypi/simple
```

### For development 

For development, it helps to use symlinks to the package source rather than install it:

```bash
$ python3 -m pip install --extra-index-url https://gitlab.com/api/v4/groups/70683489/-/packages/pypi/simple -e .
```

## Using

First you will need a token, this can be done by:

```bash
$ srcnet-oper token request
```

Which will return you an url redirecting you to IAM. 

Logging in at IAM and returning the code and state to the CLI when prompted will give you an access token. 

Valid access tokens can be listed with:

```bash
$ srcnet-oper token ls
```

From here, you can then access and functionality you are permitted to.

## Development

Makefile targets have been included to facilitate easier and more consistent development against this API. The general 
recipe is as follows:

1. Depending on the fix type, create a new major/minor/patch branch, e.g. 
    ```bash
    $ make patch-branch NAME=some-name
    ```
    Note that this both creates and checkouts the branch.
2. Make your changes.
3. Add your changes to the branch:
    ```bash
   $ git add ...
    ```
4. Either commit the changes manually (if no version increment is needed) or bump the version and commit, entering a 
   commit message when prompted:
    ```bash
   $ make bump-and-commit
    ```
5. Push the changes upstream when ready:
    ```bash
   $ make push
    ```

Note that the CI pipeline will fail if python packages with the same semantic version are committed to the GitLab 
Package Registry.
