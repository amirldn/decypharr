# Contributing / PR workflow

This fork's merge policy. Follow it for every pull request into `master`.

## Opened-by byline

Every PR body must identify who opened it, on the first line:

```
Opened by: Wes (Engineer)
```

Use the real bot or person (examples: `Wes (Engineer)`, `Nate (PR Reviewer)`, `Leon`).

## Reviewer

Nate (PR Reviewer) reviews via the GitHub App `nate-reviewer-kgbos[bot]`.

- Review **ready-for-review** PRs only.
- Skip drafts until they are undrafted.

## Merge rule

After **Nate Approves** and **CI is green**, the **PR author merges**. Squash is preferred for agent PRs.

Nate does **not** merge. Nate does **not** ask Leon to merge unless Leon explicitly asks Nate 1:1.

## Holds

An independent don't-merge review or `REQUEST_CHANGES` is a hold. Do not merge while a hold is in place.

## Branch protection

`master` has GitHub branch protection requiring at least one approving review (dismiss stale reviews).

This fork currently has little or no PR CI. Until a real PR check exists, "green CI" is a soft expectation — do not invent a fake required check name.

## Live deploy

Merging to `master` does **not** auto-deploy to Dumpster. Live promote is a separate step.
