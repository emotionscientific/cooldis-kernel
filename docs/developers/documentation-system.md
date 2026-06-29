# Documentation System

Cooldis keeps public docs in this repository and internal planning notes
outside the public release tree until they are ready to become stable contracts
or design records.

## Source Folders

- `docs/` contains the public docs surface.
- `docs/concepts/` contains introductory concepts and product direction.
- `docs/developers/` contains maintainer-facing public references.
- `site/` contains the static landing page.

The public docs are written in Markdown so they can be hosted by MkDocs,
GitHub Pages, VitePress, or another static-docs wrapper without changing the
source docs first.

## Deployment

The docs source should not depend on private planning folders. If a private note
is useful publicly, rewrite it as a stable contract and add it under `docs/`.

## Writing Style

Public docs should be declarative:

- state the product category directly;
- explain what users can do;
- describe current status honestly;
- avoid defensive comparisons unless the page is explicitly comparative;
- link internal notes only as deeper references.

Private notes can stay exploratory and argumentative. Public docs should read as
product and developer documentation.
