query1 = """
         select 
                distinct 
                        c_custkey as customer_key, 
                        c.c_mktsegment as mktsegment, 
                        try_to_number(o_orderkey) as orderkey, 
                        try_to_date(o_orderdate) as orderdate, 
                        o_orderstatus as orderstatus,
                        o_orderpriority as orderpriority,
                        to_double(o_totalprice) as totalprice          
          from snowflake_sample_data.tpch_sf1.customer c
          left join snowflake_sample_data.tpch_sf1.orders o on c.c_custkey = o.o_custkey
          order by c.c_custkey
          
         """