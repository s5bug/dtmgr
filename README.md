# dtmgr

> [!WARNING]
> this is very experimental and might completely screw up your TeX
Live install. use at your own risk.

## motivation

I wanted some way to make sure that, if my projects build locally, they will
also build in CI. For this I needed some tool that would allow me to only have
a certain set of TeX Live packages in a TeX environment.

I didn't want to install a new TeX Live for each project, and I wanted some
tool that could handle the whole process automatically.

## usage

> [!NOTE]
> for Windows users: you should use `gpedit` to change Windows Settings →
> Security Settings → Local Policy → User Rights → Creating Symbolic Links so
> that you can create symbolic links without administrator privileges.
> `dtmgr install` will fail otherwise.

First, create a `dtmgr.toml` in the root of your TeX project. It should list
packages under `dependencies`:

```toml
dependencies = [
    "scheme-basic",
    "latexmk",
    "koma-script",
]
```

Then, run `dtmgr install` to set up `.dtmgr` with the new TeX root.

Finally, you can run any command in the new TeX environment using `dtmgr run`:

```
dtmgr run lualatex main.tex
```

> [!WARNING]
> if you are using [dtmgr-action](https://github.com/s5bug/dtmgr-action) in CI,
> you do not use `dtmgr run` for CI commands. this means if using a build file,
> you likely want to i.e. manually `dtmgr run make` rather than having
> `Makefile` specify `dtmgr run` as part of steps.

## TODO

- implement progress logging
- automatically say yes to `updmap --syncwithtrees`
- replace all the `.expect`s with actual error printing
- create GitHub Action that reads the TOML to configure the global TeX Live install in CI
