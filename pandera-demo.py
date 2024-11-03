import pandera as pa
from pandera.typing import Series
import pandas as pd
from pathlib import Path
# Define the schema with correct data types
schema = pa.DataFrameSchema({
    "userid": pa.Column(
        int
    ),
    
    "movieid": pa.Column(
        str
    ),
    "rating": pa.Column(
        float,
        checks=[
            pa.Check.in_range(1, 5),
            pa.Check(lambda x: x.apply(lambda y: len(str(y).split('.')[-1])) <= 2,
                    error="Rating should have at most 1 decimal place")
        ]
    ),
    # timestamp as datetime
    "timestamp": pa.Column(
        pd.Timestamp,
        checks=[
            pa.Check(lambda x: pd.to_datetime(x).notna().all(),
                    error="Timestamp must be a valid datetime")
        ],
        coerce=True
    ),
    
    "watch_duration_minutes": pa.Column(
        int
    )
})

# point the path to the data
def retrive_movie_ratings(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# Example usage
if __name__ == "__main__":
    # read the sample data
    dataset_path = Path().absolute() / "datasets"
    data = retrive_movie_ratings(dataset_path / "sample_data.csv")
    # pandera could infer schema based on the input data
    movies_inferred_schema = pa.infer_schema(data)
    # write the inferred schema 
    with open("inferred_schema.py", "w") as file:
        file.write(movies_inferred_schema.to_script())
    try:
        # validate the data
        validated_df = schema.validate(data, lazy=True)
        print("Data validation successful!")
        
        # statistics
        print("\nData Statistics:")
        print(f"Number of records: {len(validated_df)}")
        print(f"Unique users: {validated_df['userid'].nunique()}")
        print(f"Unique movies: {validated_df['movieid'].nunique()}")
        print(f"Average rating: {validated_df['rating'].mean():.2f}")
        print(f"Average watch duration: {validated_df['watch_duration_minutes'].mean():.0f} minutes")
        
    except pa.errors.SchemaErrors as e:
        print("Validation failed!")
        print(e)