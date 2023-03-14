class SqlQueries:
     
     load_immigrants_table = ('''Insert into data.immigrants 
                                        select s.cicid,
                                               c.country,
                                               s.biryear,
                                               s.gender
                                        From  data.staging_sas_data s
                                        left join data.staging_country c
                                             ON c.i94cntyl = s.i94cit                                  ''')
     
     load_address_table = ('''Insert into data.address 
                                        select b.cicid,
                                                  b.place,
                                                  b.visa_value,
                                                  t.travel_mode
                                           From (select o.cicid,
                                                     o.place,
                                                     v.visa_value,
                                                     o.i94visa,
                                                     o.i94mode
                                                From ( select s.cicid,
                                                              a.place,
                                                              s.i94visa ,
                                                              s.i94mode
                                                         From data.staging_sas_data s
                                                         Left Join data.staging_address a
                                                         ON s.i94addr = a.i94addr   
                                                      ) as o
                                                             Left Join data.staging_visa v
                                                             ON v.i94visa = o.i94visa 
                                                 ) as b
                                                                    Left Join data.staging_travel t
                                                                    ON t.i94mode = b.i94mode
                                                                                                         ''')

     load_date_table = (''' Insert into data.date 
                                      select cicid,
                                             arrdate,
                                             EXTRACT(day FROM arrdate) as day,
                                             EXTRACT(month FROM arrdate) as month,
                                             EXTRACT(year FROM arrdate) as year
                                      From   data.staging_sas_data                                        ''')
                                