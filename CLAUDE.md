# bob-mac-sync — agent context

> **Stub.** This file exists to bind the repo work contract below. Everything else about
> this repo is still undocumented — expand it rather than assuming the absence of notes
> means there is nothing to know.

Native background sync service for BOB on macOS.

## Environments — who is on which host (canonical, ST-71542)

**BOB is pre-launch.** Do not describe it as launched, do not give anyone a launch date,
and do not treat "prod is deployed" as "we have shipped".

| Host | Firebase project | Branch | Who is on it |
|---|---|---|---|
| `bob.jc1.tech`, `bob20250810.web.app` | `bob20250810` (**dev**) | `dev` | **Jim, and only Jim.** His personal instance |
| `app.blueprint.optimise.build`, `blueprint-optimise-build-prod.web.app` | `blueprint-optimise-build-prod` (**prod**) | `main` | **Beta testers — when launch happens.** Nobody today |
| `blueprint.optimise.build` (no `app.`) | — | — | Static marketing site, separate `bob-website` repo. Nobody signs in |

Three consequences, and they are exactly what agents keep getting wrong:

1. **`bob.jc1.tech` is not production.** It is the dev project, and it is Jim's own instance.
   Any doc calling it "Production URL" is wrong.
2. **Prod is deployed and serving, but has no users on it.** Both prod hosts return 200 with
   their own bundle. "Prod returns 404" and "both domains CNAME to dev" were true once and
   are false now. Prod being live is not the same as BOB being launched.
3. **Testers belong on `app.blueprint.optimise.build`** the moment they exist. Never send a
   tester, an invite link or a sign-in URL to `bob.jc1.tech`.

So a **dev deploy disturbs nobody but Jim** — ship freely. A **prod deploy** is the one that
will be user-facing, and today still reaches no one.

Deep links in reports stay on `https://bob.jc1.tech/{goals|stories|tasks}/{id}`: that is Jim's
own instance and the only place his data lives.

**Never state what a host is serving from this table — check it:**

```bash
curl -s https://<host>/ | tr '\n' ' ' | grep -oE '__BOB_BUILD__ = \{[^}]*\}'
```


---

## Repo work contract (mandatory)

Work in this repo needs a **GitHub issue in `zulumonkeymetallic/bob-mac-sync`**, as well as the `ST-` story the work
authorisation contract in `~/.claude/CLAUDE.md` requires. The story is the commitment; the
issue is the engineering record that stays beside the diff after the story is archived.

Applies to a commit, a branch, a PR, a deploy launched from here, or a change to config,
workflow or infrastructure files. Reading code is exempt.

```bash
gh issue create --repo zulumonkeymetallic/bob-mac-sync --title "<what changes>" \
  --body "BOB story: ST-XXXXX — https://bob.jc1.tech/stories/<firestore-id>"
```

Open it before the first commit, end commit messages with `(ST-XXXXX, #123)`, and put the
issue URL on the story. Work spanning two repos gets an issue in each.

Full contract and repo map: `~/.claude/CLAUDE.md` → "Repo work contract".
