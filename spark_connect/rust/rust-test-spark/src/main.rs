use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let spark: SparkSession = SparkSessionBuilder::remote("sc://localhost:15002/")
        .build()
        .await?;

let df = spark
    .sql("SELECT * FROM 1")
    .await?;


    df.show(Some(5), None, None).await?;
    Ok(())
}