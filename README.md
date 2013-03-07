**Note**: Very rudimentary and slightly mobile specific. Eventually stuff will be refactored out of here to be a generic thing that everyone doing EL -> Limn can use
## Adding your own Graphs

(Specific to Mobile right now, limitation will be removed *soon*)

- Write an SQL Query that returns data in the appropriate format, and place it in `mobile/<name>.sql`
- Add `<name>` to appropriate position in `dashboards/reportcard.json`
- Run `generate.py mobile` to generate required metadata *and* data
- Deploy to limn! (Ask analytics to get you access)
