query1 = """
          with
              bike_share_table as (
                                   select 
                                         distinct 
                                                 cast(bs.trip_id as number) as trip_id,
                                                 cast(bs.duration as number) as duration,
                                                 to_timestamp(bs.start_time) as start_time,
                                                 to_timestamp(bs.end_time) as end_time,
                                                 cast(bs.start_station as number) as start_station,
                                                 cast(bs.start_lat as float) as start_lat,
                                                 cast(bs.start_lon as float) as start_lon,
                                                 cast(bs.end_station as number) as end_station,
                                                 cast(bs.end_lat as float) as end_lat,
                                                 cast(bs.end_lon as float) as end_lon,
                                                 bs.bike_id,
                                                 bs.trip_route_category,
                                                 bs.passholder_type
                                      from BIKE_SHARE bs
                                  )
        select * from bike_share_table 
        
         """