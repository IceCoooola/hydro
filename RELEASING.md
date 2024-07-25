# Releasing Guide

This is a guide on how to create releases for Hydroflow and all the other crates in this workspace.

We use the [`cargo-smart-release` crate](https://github.com/Byron/cargo-smart-release) for our
release workflow. Originally, cargo-smart-release [was part of gitoxide](https://github.com/Byron/gitoxide/pull/998)
but it has since been separated into its own crate. We have our own [GitHub Action release workflow](https://github.com/hydro-project/hydroflow/actions/workflows/release.yml)
([action specification here](.github/workflows/release.yml)) which is our intended way to create
releases.

Calling `cargo smart-release` is supposed to _just work_, but has a few rough edges that require a
bit of manual attention before just calling the release workflow. It is supposed to generate
changelogs automatically from our [conventional commit](https://www.conventionalcommits.org/)
messages, but requires manually intervention to do so in some situations.

## Installing `cargo-smart-release` locally

```sh
cargo install cargo-smart-release
```

## Dry run to ensure changelogs can be generated

`cargo smart-release` tries to generate changelogs from commit messages. However if a particular
package has changes but doesn't have the right commit messages then `cargo smart-release` will
complain and give up.

To see if anything needs addressing, go to the [Release action](https://github.com/hydro-project/hydroflow/actions/workflows/release.yml)
and click on the "Run workflow" button in the top right corner. Branch should be `main`, version
bump should most likely be `patch`, `minor`, or `major`. Note that semantic versioning is:
```js
    {major}.{minor}.{patch}
```
(Sometimes you might use the `keep` version bump if you have manually changed all the packages'
`Cargo.toml` versions and commited that. But I don't remember exactly how that works.)

Make sure to leave "Actually execute and publish the release?" **UNCHECKED** for a dry test run. If
all goes well the action job should complete successfully (with a green check) in about 9 minutes,
and the log under "Release Job" > "Run cargo smart-release" should look something like below,
showing that all the changelogs can be modified. Make sure the version bumps look correct too.

```log
[INFO ] Updating crates-io index
[WARN ] Refused to publish 'hydroflow_cli_integration' as as it didn't change.
[INFO ] Will not publish or alter 3 dependent crates: unchanged = 'hydroflow_cli_integration', 'variadics', 'pusherator'
[INFO ] WOULD auto-bump dependent package 'hydroflow_lang' from 0.4.0 to 0.5.0 for publishing
[INFO ] WOULD auto-bump dependent package 'hydroflow_datalog_core' from 0.4.0 to 0.5.0 for publishing, for SAFETY due to breaking package 'hydroflow_lang'
[INFO ] WOULD auto-bump dependent package 'hydroflow_datalog' from 0.4.0 to 0.5.0 for publishing, for SAFETY due to breaking package 'hydroflow_datalog_core'
[INFO ] WOULD auto-bump dependent package 'hydroflow_macro' from 0.4.0 to 0.5.0 for publishing, for SAFETY due to breaking package 'hydroflow_lang'
[INFO ] WOULD auto-bump dependent package 'lattices' from 0.4.0 to 0.5.0 for publishing
[INFO ] WOULD minor-bump provided package 'hydroflow' from 0.4.0 to 0.5.0 for publishing, for SAFETY due to breaking package 'hydroflow_datalog'
[INFO ] WOULD minor-bump provided package 'hydro_cli' from 0.4.0 to 0.5.0 for publishing
[INFO ] WOULD adjust 2 manifest versions due to breaking change in 'hydroflow_lang': 'hydroflow_datalog_core' 0.4.0 ➡ 0.5.0, 'hydroflow_macro' 0.4.0 ➡ 0.5.0
[INFO ] WOULD adjust 1 manifest version due to breaking change in 'hydroflow_datalog_core': 'hydroflow_datalog' 0.4.0 ➡ 0.5.0
[INFO ] WOULD adjust 1 manifest version due to breaking change in 'hydroflow_datalog': 'hydroflow' 0.4.0 ➡ 0.5.0
[INFO ] WOULD adjust version constraints in manifests of 2 packages as direct dependencies are changing: relalg, website_playground
[INFO ] WOULD modify existing changelog for 'hydroflow_lang'.
[INFO ] WOULD modify existing changelog for 'hydroflow_datalog_core'.
[INFO ] WOULD modify existing changelog for 'hydroflow_datalog'.
[INFO ] WOULD modify existing changelog for 'hydroflow_macro'.
[INFO ] WOULD modify existing changelog for 'lattices'.
[INFO ] WOULD modify existing changelog for 'hydroflow'.
[INFO ] WOULD modify existing changelog for 'hydro_cli'.
```

If the job does not succeed or succeeds but fails to generate changelogs for certain packages, then you will
need to do a bit of manual work. That looks like this in the log (check for this!):
```log
[WARN ] WOULD ask for review after commit as the changelog entry is empty for crates: hydroflow_datalog, hydroflow_macro
```
In this case, you will need to create a commit to each package's `CHANGELOG.md` to mark it as
unchanged (or minimally changed). For example, [hydro_datalog 0.4](https://github.com/hydro-project/hydroflow/commit/5faee64ab82eeb7a24f62a1b55c46d72d8eb5320)
or [hydro_cli 0.3](https://github.com/hydro-project/hydroflow/commit/4c2cf81411835529b5d7daa35717834e46e28b9b).

## Real-deal run

Again, go to the [Release action](https://github.com/hydro-project/hydroflow/actions/workflows/release.yml)
and click on the "Run workflow" button in the top right corner. Select branch `main`, version bump as needed and this time _check_ the "Actually execute and publish the release?" box.

Hopefully all goes well and the release will appear on the other end.

If the release fails it may leave the repo in a bit of a half-broken or half-released state. Some
or all of the release verison tags may be pushed. You may need to manually create some
[GitHub releases](https://github.com/hydro-project/hydroflow/releases).
You can also try re-running the release action but with the version bump set to `keep`, if versions
have been bumped but not released. You'll have to figure it out, its finicky.


**DO NOT MAKE CHANGES TO `main` WHEN THE RELEASE WORKFLOW IS RUNNING!**

If you make changes to main, then the release workflow may fail at the very end when it tries to
push its generated commits to `main`. The job should've pushed some commit with a bunch of version
tags and you (probably) need to hard-reset main to point to that tagged commit instead of whatever
junk you mistakenly pushed.

## Addendum: Adding new crates

When adding a new crate which is published, you need to:
1. Ensure `publish = true` and other required fields (`license`, `description`, `documentation`,
   etc.), are set in `my_crate/Cargo.toml`
   https://doc.rust-lang.org/cargo/reference/publishing.html#before-publishing-a-new-crate
2. Ensure any `path` dependencies to/from `my_crate` also include `version = "^0.1.0"`
   (substitute correct version).
3. You must commit a new (empty) file `my_crate/CHANGELOG.md` to ensure the file will be tracked
   by git and pushed by `cargo-smart-release`
4. If you want your package to be lockstep-versioned alongside hydroflow then make sure to add it
   to the [command in the `release.yml` workflow](https://github.com/hydro-project/hydroflow/blob/main/.github/workflows/release.yml#L82).

Then just run the release workflow as normal.

## Addendum: Moving crates

`cargo-smart-release` automatically generates changelogs. However it only looks for changes in the
package's _current_ directory, so if you move a package to a different directory then the changelog
may lose old commit info if you're not careful.

On the commit immediately _before_ you move the package(s) and run the following:
```
cargo changelog --write <crate_to_be_moved> <other_crate_to_be_moved> ...
```
Then, before committing the changes, go through the modified `CHANGELOG.md` files and add a prefix
to the `Commit Statistics` and `Commit Details` headers, for example: `Pre-Move Commit Statistics`/`Pre-Move Commit Details`.
This is necessary because otherwise `cargo-smart-release` will treat those sections as auto-generated
and will not preserve them, but then won't regenerate them due to the package moving. Commit the
updated changelogs and cherry-pick that commit to the latest version if you went back in history.
The changelogs should now be safely preserved by future releases.

## Addendum: The GitHub App account

So... `cargo smart-release` wants to push to `hydro-project/hydroflow`'s `main` branch. However,
branch protection says you can only push to main via a pull request, and for some reason that
branch protection also applies to GitHub Actions.

To get around this problem we created a [separate GitHub App account called Hydro Project Bot](https://github.com/organizations/hydro-project/settings/apps/hydro-project-bot).
Basically it is a pretty unremarkable unpublished GitHub App with permissions to modify repos.
It has some sort of secret which lets us act as the app within GitHub actions, which is passed
through via `secrets.APP_PRIVATE_KEY`. (I guess this is the "Client secrets" secret, but for some
reason that says "Never used"? I don't remember). Importantly, we have also given the Hydro Project
Bot permission to bypass [`main` branch protection rules](https://github.com/hydro-project/hydroflow/settings/branch_protection_rules/24797446),
under "Allow specified actors to bypass required pull requests" and also under "Allow force pushes"
(although I don't think that `cargo smart-release` does force pushes?).

Anyway, at some point that `APP_PRIVATE_KEY` secret will expire and we'll need to regenerate and
reset it. Good luck.
