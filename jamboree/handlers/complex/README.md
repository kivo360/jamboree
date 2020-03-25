# Complex Handlers

Complex handlers have multiple types included inside that need to be synced. They're more common inside of Linkkt's proprietry systems. These will be systems we'll leave a bit more exposed. A good set of examples:

1. MetaHandler
   1. Will keep track of all metadata for our system
   2. Metadata will be overwritable records and also include the future search handler
   3. The search handler will help us find associated data
2. Metric
   1. The metric handler will extend from the DBhandler, through it'll also 
      1. Searchable by including our MetaHandler 
      2. Attach our TimeHandler so we can dynamically backtest and see a model perform over time
3. ModelHandler
   1. Will extend from the BlobHandler
   2. It'll also include a `MetricHandler` and `MetaHandler`
   3. The `MetricHandler` will allow us to monitor predictions are progressing over time
      1. Since this has a `TimeHandler` included we'll be able to track metrics for a given model
      2. Since this also has its own designated `MetaHandler` we'll be able to search for a model's effectiveness over time from a different system.
   4. The `MetaHandler` will also help us find the model later