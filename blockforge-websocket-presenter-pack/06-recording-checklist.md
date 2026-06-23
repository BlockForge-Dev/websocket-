# Recording Checklist

## Repository preparation

- complete the milestone being discussed
- ensure the default branch is green
- remove secrets and local environment files
- use realistic but non-sensitive sample data
- create a release or milestone tag
- verify README setup steps from a clean environment
- keep architecture docs and message examples synchronized with code

## Editor preparation

- increase font size
- hide minimap and unrelated panels
- close unrelated files
- prepare exact symbols in editor tabs
- use a clean terminal prompt
- increase terminal font size
- clear old command output
- disable distracting notifications

## Slide preparation

- use one major idea per slide
- show component responsibilities, not decorative arrows
- keep labels consistent with repository names
- distinguish current architecture from future architecture
- label guarantees and exclusions
- use the same message names in slides, code, tests, and README

## Technical checks

- confirm heartbeat timings match proxy assumptions
- confirm sender-included broadcast policy
- confirm duplicate-user policy
- confirm queue capacity and overflow policy
- confirm offline private-message behavior
- confirm origin and authentication behavior
- confirm health and readiness semantics
- confirm all code references still exist

## Presentation checks

- state the delivery guarantee clearly
- separate authentication from authorization
- distinguish socket acceptance from user delivery
- explain why the hub does not own business logic
- explain why the hub does not write directly to sockets
- show one failure path, not only happy paths
- state what version one deliberately excludes
- return to the full architecture after every code cutaway

## Final review

- no claim exceeds what the implementation proves
- no diagram contradicts the repository
- no code section becomes a line-by-line syntax lesson
- every milestone has a behavioral definition of done
- links to code, slides, and architecture notes work
- the closing points to the next concrete episode

