"""
    Table class module to allow standard interfacing with Athena tables
"""
import datetime
import os
import pathlib
import dateutil
import boto3
import pandas
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.schema import Table, MetaData


_s3 = boto3.client(service_name="s3", region_name="us-east-1")
_glue = boto3.client(service_name="glue", region_name="us-east-1")
_athena = boto3.client(service_name="athena", region_name="us-east-1")
_ALLOWED_LOCATIONS = ("my_folder", "my_another_folder")
_DEFAULT_BUCKET = "my_default_bucket"
_home = os.path.expanduser("~")
_today = datetime.date.today().isoformat().replace("-", "/")
_yesterday = (
    (datetime.date.today() + dateutil.relativedelta.relativedelta(days=-1))
    .isoformat()
    .replace("-", "/")
)
_lastmonth = (
    (datetime.date.today() + dateutil.relativedelta.relativedelta(months=-1))
    .isoformat()
    .replace("-", "/")
)

class AthenaTable:
    """
    This class allows for a standard way to interface with Athena tables
    """
	def __init__(self, table_schema, table_name, access_type, source_type):
	    self.table_schema = table_schema
	    self.table_name = table_name
	    self.access_type = AccessType.READONLY
	    self.source_type = SourceType.INCOMING
	    self.source_path = None
	    self.table_bucket = _DEFAULT_BUCKET
	    self.glue_definition = field(default_factory=dict)
	    self.table_type = PrestoTableType.UNKNOWN
   
    def cast_dataframe(self, frame):
        for column in self.glue_definition["Table"]["StorageDescriptor"]["Columns"]:
            if column["Name"] in frame.columns:
                col = column["Name"]
                if (
                    col.endswith("date")
                    or col.endswith("_dt")
                    or col.endswith("_ts")
                    or col.endswith("_date_time")
                    or col.endswith("_timestamp")
                ):
                    frame[col] = pandas.to_datetime(frame[col])
                if col.__contains__("zip_cd") or col.__contains__("cbsa_cd"):
                    frame[col] = frame[col].apply(
                        lambda geo_cd: zfill(geo_cd, 5)
                    )
                if col.__contains__("cnty_cd"):
                    frame[col] = frame[col].apply(
                        lambda geo_cd: zfill(geo_cd, 3)
                    )
                if col.__contains__("sale_regn_cd"):
                    frame[col] = frame[col].apply(
                        lambda geo_cd: zfill(geo_cd, 2)
                    )
                if col.__contains__("prim_msa_cd"):
                    frame[col] = frame[col].apply(
                        lambda geo_cd: zfill(geo_cd, 4)
                    )
                if col.__contains__("prim_census_tract_cd"):
                    frame[col] = frame[col].apply(
                        lambda geo_cd: zfill(100 * geo_cd, 6)
                    )
                if col.__contains__("customer_id") or col.__contains__("_cust_id"):
                    frame[col] = frame[col].apply(
                        lambda customer_id: zfill(customer_id, 10)
                    )
                if col.__contains__("lnprps_cd") or col.__contains__("uw_desgntn_cd"):
                    frame[col] = frame[col].apply(
                        lambda unk_cd: unk_cd
                        if unk_cd == "UNK"
                        else zfill(unk_cd, 2)
                    )
                frame[column["Name"]] = cast_series_type(
                    frame[column["Name"]], column["Type"]
                )
        return frame


    def repair(self) -> None:
        """
        This function uses SQLAlchemy to issue a MSCK REPAIR TABLE for a table in athena.

        It checks for a table definition.
        It makes sure it is a table and not a view.
        It runs MSCK REPAIR TABLE.
        """
        self.set_glue_dictionary()
        assert self.glue_definition, "You are trying to repair a non-defined TABLE!"
        assert self.presto_table_type not in (
            PrestoTableType["VIRTUAL_VIEW"],
        ), "You are trying to repair a non-defined TABLE!"
        with self.mgic_athena_engine().connect() as conn:
            conn.execute(f"MSCK REPAIR TABLE {self.table_schema}.{self.table_name};")

    def drop(self) -> None:
        """
        This method uses SQLAlchemy to drop the table

        It checks that the access allows for dropping the table.
        If so, it drops the table using the SQLAlchemy Table object.
        """
        assert (
            self.access_type == AccessType.OVERWRITE
        ), f"You are trying to drop a table ({self}) with table_type != {AccessType.OVERWRITE}"
        if self.glue_definition:
            sa_table = Table(
                self.table_name, MetaData(bind=self.mgic_athena_engine()), autoload=True
            )
            sa_table.drop()
            self.glue_definition = dict()

    def list_objects(self):
        """
        This method returns a list of s3 paths of all the parquet files supporting the table
        """
        ans = []
        self.set_glue_dictionary()
        if self.presto_table_type in [
            PrestoTableType.VIRTUAL_VIEW,
            PrestoTableType.UNKNOWN,
        ]:
            ans = []
        else:
            location = self.glue_definition["Table"]["StorageDescriptor"]["Location"]
            bucket, prefix = location.split("//")[-1].split("/", 1)
            objects = s3_list_objects(Bucket=bucket, Prefix=prefix)
            if "Contents" in objects.keys():
                ans = [_["Key"] for _ in objects["Contents"]]
            else:
                pass
        return ans

    
    def empty(self) -> None:
        """
        This function uses the glue_definition of the table to delete the objects supporting the table

        It gets the table information using glue.  It checks that the table access allows for emptying.
        It finds all the files to delete and deletes them.
        Finally, it updates the glue_definition, and repairs the table.
        """
        self.set_glue_dictionary()
        assert (
            self.access_type == AccessType.OVERWRITE
        ), f"You are trying to empty a table ({self}) with table_type != {AccessType.OVERWRITE}"
        print(
            f"EMPTYING: {self.table_schema}.{self.table_name} AT {self.s3_root}/{self.s3_table_path} !"
        )
        for _ in self.list_objects():
            _s3.delete_object(Bucket=f"{self.table_bucket}", Key=_)
        self.set_glue_dictionary()
        self.repair()

    def purge(self) -> None:
        """
        This is just an empty followed by a drop
        """
        self.set_glue_dictionary()
        if self.glue_definition:
            self.empty()
            self.drop()


    def populate_from_source(self, ymd=_today, files_are_parquet=True):
        """
        This method populates a table by copying that data from the source (specified at the table's definition)
        """
        self.set_glue_dictionary()
        if self.source_type in {SourceType.HTTP}:
            data_frame = pandas.read_csv(self.source_path)
            data_frame.columns = [_.lower() for _ in data_frame.columns]
            data_frame = self.castframe(data_frame)
            data_frame.to_parquet(
                f"{self.glue_definition['Table']['StorageDescriptor']['Location']}/bulk.parquet",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )
            self.set_glue_dictionary()
        elif self.source_type == SourceType.PRODUCTION:
            files = [_ for _ in self.listen(files_are_parquet) if _.__contains__(ymd)]
            if files:
                self.empty()
                for _file in files:
                    source_bucket = "my-prod-bucket"
                    _s3.copy(
                        {"Bucket": source_bucket, "Key": _file},
                        Bucket=self.table_bucket,
                        Key=f"{self.s3_root}/{self.table_schema}/{self.table_name}/{_file.split('/',1)[-1]}",
                    )
        elif self.source_type in {SourceType.INCOMING, SourceType.LEGACY}:
            files = self.listen()
            if files:
                self.empty()
                for _file in files:
                    source_bucket = "DONOTDELETEME"
                    if self.source_type == SourceType.INCOMING:
                        source_bucket = _DEFAULT_BUCKET
                        _s3.copy(
                            {"Bucket": source_bucket, "Key": _file},
                            Bucket=self.table_bucket,
                            Key=f"{self.s3_root}/{self.table_schema}/{self.table_name}/{_file.split('/',1)[-1]}",
                        )
                    elif self.source_type == SourceType.LEGACY:
                        source_bucket = "legacy-raw-data-qa"
                        if _current_file(_file):
                            _s3.copy(
                                {"Bucket": source_bucket, "Key": _file},
                                Bucket=self.table_bucket,
                                Key=f"{self.s3_root}/{self.table_schema}/{self.table_name}/{_file.split('/',1)[-1]}",
                            )
                        else:
                            pass
                    else:
                        pass
                    if source_bucket == _DEFAULT_BUCKET:
                        _s3.delete_object(Bucket=f"{self.table_bucket}", Key=_file)
                    else:
                        pass
            else:
                pass
        else:
            pass
        self.repair()

    

    def build_from_sql(self):
        """
        This method build a table defined via SQL
        """
        self.set_glue_dictionary()
        assert (
            self.source_type == SourceType.ATHENA
        ), f"You are trying to build from SQL a table whose source_type != {SourceType.ATHENA}"
        with open(self.source_path, "r") as sqlin:
            stmt = sqlin.read()
        self.purge()
        with self.mgic_athena_engine().connect() as conn:
            conn.execute(stmt)
        self.set_glue_dictionary()
        self.repair()

    def refresh_from_sql(self):
        """
        This method refreshes a table that was defined using an SQL statement.
        TODO:
            1. check whether purging the table is a necessary step
            2. how is this method currently used
        """
        self.set_glue_dictionary()
        assert (
            self.source_type == SourceType.ATHENA
        ), f"You are trying to build from SQL a table whose source_type != {SourceType.ATHENA}"
        with open(self.source_path, "r") as sqlin:
            stmt = sqlin.read()
        with self.mgic_athena_engine().connect() as conn:
            conn.execute(stmt)
        self.set_glue_dictionary()

    


	def tsvgz_to_parquet(table_name, source_alias=None):
	    """
	    This function will load tsvgz files into memory one at a time
	    and generate pandas.DataFrames
	    """
	    if source_alias is None:
		    source_alias = table_name
	        tbl = Trapezi(
		    "mgic_analytics", table_name, AccessType.OVERWRITE, SourceType.PRODUCTION
	        )
	        tbl.set_glue_dictionary()
	    if tbl.glue_definition:
    		pass
	    else:
    		tbl.define_external("processed", "Incoming_MetaData")
	    source_table = Trapezi(
		"staging", source_alias, AccessType.OVERWRITE, SourceType.PRODUCTION
	    )
	    newfiles = [_ for _ in source_table.listen() if _.__contains__(_today)]
	    if newfiles:
		    tbl.empty()
		for counter, source_path in enumerate(newfiles):
		    write_parquet_path = f"s3://{tbl.table_bucket}/{tbl.s3_root}/{tbl.table_schema}/{tbl.table_name}/bulk_{str(counter).zfill(2)}.parquet"
		    read_parquet_path = f"s3://{source_table.source_bucket}/{source_path}"
		    frame = pandas.read_table(read_parquet_path)
		    frame.columns = tbl.columns
		    frame = tbl.castframe(frame)
		    frame.to_parquet(
		        write_parquet_path,
		        coerce_timestamps="ms",
		        allow_truncated_timestamps=True,
		    )
		tbl.repair()


	def simple_production(tablename):
	    """
	    This function allows for the simple ingestion of table for which IT provides parquet files
	    and that require no further transformations
	    """
	    tbl = Trapezi(
		"mgic_analytics", tablename, AccessType.OVERWRITE, SourceType.PRODUCTION
	    )
	    tbl.set_glue_dictionary()
	    if tbl.glue_definition:
		    pass
	    else:
    		tbl.define_external("processed", "Incoming_MetaData")
	        tbl.populate_from_source()
	    
	    
	    
	@property
    def s3_table_path(self):
        """
        This property reveals the s3 path of the parquet files supporting the table
        """
        ans = None
        self.set_glue_dictionary()
        if self.glue_definition:
            ans = (
                self.glue_definition["Table"]["StorageDescriptor"]["Location"]
                .split(self.table_bucket)[1]
                .split("/", 2)[2]
            )
        else:
            pass
        return ans

    @property
    def columns(self):
        """
        This property reveals the columns of a table as they are in Athena
        """
        ans = []
        self.set_glue_dictionary()
        if self.glue_definition:
            ans = [
                _["Name"]
                for _ in self.glue_definition["Table"]["StorageDescriptor"]["Columns"]
            ]
        else:
            pass
        return ans
