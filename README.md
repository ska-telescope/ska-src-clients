# SKA SRC Clients

Command line interfaces to SRCNet APIs.

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

or use the Makefile target `make install-local`. This allows for testing without reinstalling the package.

**If changes have been made to any of the APIs, you may need to update the pinned requirements version for the API
and uninstall/reinstall the package.**

## Usage

The following command line interfaces are currently available:
 - srcnet-oper: Low level **oper**ational commands.

### srcnet-oper

#### SRCNet API authentication

Most of the interfaces sit behind authorisation. Authorisation is granted by providing a token retrieved from 
authenticating with the SRCNet APIs.

To get a token, issue a request:

```bash
$ srcnet-oper token request
```

which will return you an url redirecting you to IAM. Logging in to IAM and returning the code and state to the CLI
when prompted will give you an access token. 

Access and refresh tokens are stored locally on disk. By default the path is `/tmp/srcnet/user`.

Valid access tokens can be listed with:

```bash
$ srcnet-oper token ls
```

After authorisation, you can proceed to run any of the commands (provided you have the required permission). A full 
list of commands is available in the self-generated 
[documentation](https://ska-telescope.gitlab.io/src/src-service-apis/ska-src-clients/srcnet-oper.html#Sub-commands), or 
you can use the `--help` flag.

```bash
$ srcnet-oper --help
```

#### 

## Development

Makefile targets have been included to facilitate easier and more consistent development against this package. The 
general recipe is as follows:

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

## Reference

1. [CLI documentation](https://ska-telescope.gitlab.io/src/src-service-apis/ska-src-clients/index.html).
