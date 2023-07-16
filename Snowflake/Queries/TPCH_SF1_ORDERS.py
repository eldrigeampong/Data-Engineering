query1 = '''select
                  distinct 
                          o.o_orderkey,
                          o.o_custkey,
                          o.o_orderstatus,
                          o.o_totalprice,
                          o.o_orderdate,
                          o.o_orderpriority
            from
              ORDERS o
              
        '''