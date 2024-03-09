import ncas_amof_netcdf_template as nant
import polars as pl
import datetime as dt


def read_data_year(input_file, year=2023):
    df = pl.read_csv(
        input_file,
        columns = [
            "Date","Relative_Humidity","Relative_Humidity_Flag","Temperature",
            "Temperature_Flag","Irradiance","Irradiance_Flag","Net_Irradiance",
            "Net_Irradiance_Flag","Wind_Speed","Wind_Speed_Flag","Wind_Direction",
            "Wind_Direction_Flag","Atmospheric_Pressure","Atmospheric_Pressure_Flag"
        ],
        dtypes=[str,str,str,str,str,str,str,str,str,str,str,str,str,str,str]
    )
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    dt_dates = []
    for i in df["Date"]:
        if i.count(":") == 1:
            dt_dates.append(dt.datetime.strptime(i, "%d/%m/%Y %H:%M"))
        else:
            dt_dates.append(dt.datetime.strptime(i, "%d/%m/%Y %H:%M:%S"))
    dt_dates = pl.Series("dt_dates", dt_dates)
    df = df.with_columns(dt_dates.alias("Date"))
    df = df.filter(pl.col("Date").is_between(dt.datetime(year,1,1), dt.datetime(year,12,31,23,59,59,999999) ))

    for c in df.columns[1:]:
        vals = pl.Series("val", [ -1e20 if i == "NULL" else float(i) for i in df[c] ])
        df = df.with_columns(vals.alias(c))

    return df


def make_netcdf(input_file="uea-aws-2_growing.csv", output_location=".", product_version="1.0", year=2023):
    data_df = read_data_year(input_file, year=year)
    unix_times, day_of_year, years, months, days, hours, minutes, seconds, \
      time_coverage_start_unix, time_coverage_end_unix, file_date = nant.util.get_times(data_df["Date"])
    
    nc = nant.create_netcdf.main("uea-aws-2", date = file_date, dimension_lengths={"time":len(unix_times)}, file_location = output_location, product_version = product_version)

    nant.util.update_variable(nc, "time", unix_times)
    nant.util.update_variable(nc, "day_of_year", day_of_year)
    nant.util.update_variable(nc, "year", years)
    nant.util.update_variable(nc, "month", months)
    nant.util.update_variable(nc, "day", days)
    nant.util.update_variable(nc, "hour", hours)
    nant.util.update_variable(nc, "minute", minutes)
    nant.util.update_variable(nc, "second", seconds)

    nant.util.update_variable(nc, "air_pressure", data_df["Atmospheric_Pressure"])
    nant.util.update_variable(nc, "air_temperature", data_df["Temperature"])
    nant.util.update_variable(nc, "relative_humidity", data_df["Relative_Humidity"])
    nant.util.update_variable(nc, "wind_speed", data_df["Wind_Speed"])
    nant.util.update_variable(nc, "wind_from_direction", data_df["Wind_Direction"])
    nant.util.update_variable(nc, "downwelling_total_irradiance", data_df["Irradiance"])
    nant.util.update_variable(nc, "net_total_irradiance", data_df["Net_Irradiance"])

    if len(data_df.filter(pl.col("Atmospheric_Pressure") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_pressure", data_df["Atmospheric_Pressure_Flag"])
    if len(data_df.filter(pl.col("Temperature") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_temperature", data_df["Temperature_Flag"])
    if len(data_df.filter(pl.col("Relative_Humidity") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_relative_humidity", data_df["Relative_Humidity_Flag"])
    if len(data_df.filter(pl.col("Wind_Speed") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_wind_speed", data_df["Wind_Speed_Flag"])
    if len(data_df.filter(pl.col("Wind_Direction") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_wind_from_direction", data_df["Wind_Direction_Flag"])
    if len(data_df.filter(pl.col("Irradiance") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_downwelling_total_irradiance", data_df["Irradiance_Flag"])
    if len(data_df.filter(pl.col("Net_Irradiance") != -1e20)) > 0:
        nant.util.update_variable(nc, "qc_flag_net_total_irradiance", data_df["Net_Irradiance_Flag"])

    nc.setncattr('time_coverage_start',
                 dt.datetime.fromtimestamp(time_coverage_start_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
    nc.setncattr('time_coverage_end',
                 dt.datetime.fromtimestamp(time_coverage_end_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
    
    nant.util.add_metadata_to_netcdf(nc, 'metadata.csv')
    
    # Close file
    nc.close()

    # Check for empty variables and remove if necessary
    nant.remove_empty_variables.main(f'{output_location}/uea-aws-2_wao_{file_date}_surface-met_v{product_version}.nc')

if __name__ == "__main__":
    import sys
    input_file = sys.argv[1]
    output_loc = sys.argv[2]
    product_version = sys.argv[3]
    year = int(sys.argv[4])
    make_netcdf(input_file=input_file, output_location=output_loc, product_version=product_version, year = year)
