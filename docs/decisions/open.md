# Open architecture decisions

These items require an Architecture decision. They are not implementation
freedom and do not weaken any settled contract.

## API graph model completion

The settled core [`GraphView`, `GraphDelta`, `GraphHistory`, and
`GraphPublication` model](../architecture/api.md#graph-views-publication-revision-and-history--incomplete-working-contract)
remains incomplete in one area. Original completion-item numbering is
preserved:

6. the consistent database query and projection that constructs a revision-zero
   `GraphView`, based on KGI v1.

Settle these parts one at a time in the API architecture. The completed design
must preserve the already settled graph-model core linked above and the public
API resource-isolation contract.
