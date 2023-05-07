from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from century import century
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task")
                     .config(conf=SparkConf())
                     .getOrCreate())
    akas_schema = t.StructType([
        t.StructField(c.title_id, t.StringType(), True),
        t.StructField(c.ordering, t.StringType(), True),
        t.StructField(c.title, t.StringType(), True),
        t.StructField(c.region, t.StringType(), True),
        t.StructField(c.language, t.StringType(), True),
        t.StructField(c.types, t.StringType(), True),
        t.StructField(c.attributes, t.StringType(), True),
        t.StructField(c.is_original_title, t.StringType(), True)])
    akas_df = spark_session.read.csv('C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/title.akas.tsv.gz',
                                     sep="\t", header=True, nullValue='Null', schema=akas_schema)
    akas_df.filter(f.col('region') == 'UA').select('title', 'region').write.csv(
        'C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/1', header=True, mode='overwrite',
        encoding='windows-1251')
    akas_df.write.csv('C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/123', header=True, mode='overwrite')
    names_schema = t.StructType([
        t.StructField(c.nconst, t.StringType(), True),
        t.StructField(c.primary_name, t.StringType(), True),
        t.StructField(c.birth_year, t.IntegerType(), True),
        t.StructField(c.death_year, t.IntegerType(), True),
        t.StructField(c.primary_profession, t.StringType(), True),
        t.StructField(c.known_for_titles, t.StringType(), True)])
    names_df = spark_session.read.csv('C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/name.basics.tsv.gz',
                                      sep="\t", header=True, nullValue='Null', schema=names_schema)
    names_df = names_df.withColumn('century', f.col('birth_year') / 100)
    names_df = names_df.withColumn('century', names_df.century.cast(t.IntegerType()))
    names_df.filter(f.col('century') == '18').select('primary_name', 'birth_year').write.csv(
        'C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/2', header=True, mode='overwrite')
    title_2_hours_schema = t.StructType([
        t.StructField(c.tconst, t.StringType(), True),
        t.StructField(c.title_type, t.StringType(), True),
        t.StructField(c.primary_title, t.StringType(), True),
        t.StructField(c.original_title, t.StringType(), True),
        t.StructField(c.adult, t.StringType(), True),
        t.StructField(c.start_year, t.DateType(), True),
        t.StructField(c.end_year, t.DateType(), True),
        t.StructField(c.runtime_minutes, t.IntegerType(), True),
        t.StructField(c.genres, t.StringType(), True)])
    title_2_hours_df = spark_session.read.csv(
        'C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/title.basics.tsv.gz',
        sep="\t", header=True, nullValue='Null', schema=title_2_hours_schema)
    title_2_hours_df = title_2_hours_df.withColumn('runtime_hours', f.col('runtime_minutes') / 121)
    title_2_hours_df = title_2_hours_df.withColumn('runtime_hours',
                                                   title_2_hours_df.runtime_hours.cast(t.IntegerType()))
    title_2_hours_df.filter((f.col('runtime_hours') != 0) & (f.col('title_type') == 'movie')).select('title_type',
                                                                                                     'primary_title',
                                                                                                     'original_title',
                                                                                                     'runtime_minutes').write.csv(
        'C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/3', header=True, mode='overwrite')
    names_characters_schema = t.StructType([
        t.StructField(c.tconst, t.StringType(), True),
        t.StructField(c.ordering, t.IntegerType(), True),
        t.StructField(c.nconst, t.StringType(), True),
        t.StructField(c.category, t.StringType(), True),
        t.StructField(c.job, t.StringType(), True),
        t.StructField(c.characters, t.StringType(), True)])
    names_characters_df = spark_session.read.csv(
        'C:/Users/Dima/PycharmProjects/imdb-spark-project/Data/title.principals.tsv.gz',
        sep="\t", header=True, nullValue='Null', schema=names_characters_schema)
    names_characters_df.sort(f.col('nconst')).show()


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
