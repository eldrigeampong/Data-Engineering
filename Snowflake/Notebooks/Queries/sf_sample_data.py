query1 = """
        select 
              distinct 
                      c_custkey as customer_key, 
                      c.c_mktsegment as mktsegment, 
                      o_orderkey as orderkey, 
                      o_orderdate as orderdate, 
                      o_orderstatus as orderstatus,
                      o_orderpriority as orderpriority,
                      o_totalprice as totalprice          
        from snowflake_sample_data.tpch_sf1.customer c
        left join snowflake_sample_data.tpch_sf1.orders o on c.c_custkey = o.o_custkey
        order by c.c_custkey
      
        """