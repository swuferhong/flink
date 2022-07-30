/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- start query 1 in stream 0 using template query32.tpl and seed 2031708268
select  sum(cs_ext_discount_amt)  as `excess discount amount`
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 269
and i_item_sk = cs_item_sk 
and d_date between '1998-03-18' and 
        (cast('1998-03-18' as date) + interval '90' day)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1998-03-18' and
                             (cast('1998-03-18' as date) + interval '90' day)
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100

-- end query 1 in stream 0 using template query32.tpl
