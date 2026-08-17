# bob-mac-sync — agent context

> **Stub.** This file exists to bind the repo work contract below. Everything else about
> this repo is still undocumented — expand it rather than assuming the absence of notes
> means there is nothing to know.

Native background sync service for BOB on macOS.

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
