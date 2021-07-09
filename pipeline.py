from sklearn.pipeline import Pipeline
import tasks
import warnings

warnings.filterwarnings("ignore")

# Defining the steps for pipeline
pipe = Pipeline(steps=[
    ('Database-Connection', tasks.connection()),
    ('Aggregating-Data', tasks.aggregate_data())
])

# Triggering the sklearn pipeline
pipe
