import psycopg2


def filter_model(year, list_parameters):
    filter_po = f""" SELECT DISTINCT (po.item_id)
    FROM program_object po
    LEFT JOIN program p ON p.item_id=po.program_item_id --программы работ
    LEFT JOIN dic.region r ON r.item_id=po.region_item_id --регионы
    LEFT JOIN dic.district d ON d.item_id=r.district_item_id --фед округа
    LEFT JOIN child_object co ON co.object_id=po.item_id --для связи с дорогами
    LEFT JOIN dic.road_section rs ON rs.item_id=co.road_section_item_id --для связи с дорогами2
    LEFT JOIN dic.fku ON fku.item_id = po.fku_item_id -- фку для параметра is_active
    WHERE (po.start_year <= {year} AND po.end_year >= {year}) --год
    AND po.status_guid = '2a833f15-416b-40a9-8840-cc9781a60a08'--Утверждено
    AND p.status = 'Утверждена' --ПР утверждена
    AND dic.fku.is_active = TRUE -- только активные фку
    {list_parameters['fku_switch']}AND po.fku_item_id = '{list_parameters['fku']}' --(фильтр по фку) ФКУ Упрдор «Северо-Запад»
    {list_parameters['district_switch']}AND d.item_id = '{list_parameters['district']}' --(фильтр по фед округам) Северо-Западный федеральный округ
    {list_parameters['region_switch']}AND r.item_id = '{list_parameters['region']}' --(фильтр по региону) Ленинградская область
    {list_parameters['program_type_switch']}AND po.program_type_item_id = '{list_parameters['program_type']}' --(фильтр по ПР) Содержание  автомобильных дорог
    {list_parameters['road_switch']}AND ((po.item_id = co.object_id) AND (COALESCE (rs.highway_section_item_id, '00000000-0000-0000-0000-000000000000') = '{list_parameters['road']}')) --(фильтр по дороге) Санкт-Петербург - Псков - Пустошка- Невель - граница с Республикой Белоруссия  """
    filter_po_correct = f"""SELECT DISTINCT (po.item_id)
    FROM program_object po
    LEFT JOIN program p ON p.item_id=po.program_item_id --программы работ
    LEFT JOIN dic.region r ON r.item_id=po.region_item_id --регионы
    LEFT JOIN dic.district d ON d.item_id=r.district_item_id --фед округа
    LEFT JOIN child_object co ON co.object_id=po.item_id --для связи с дорогами
    LEFT JOIN dic.road_section rs ON rs.item_id=co.road_section_item_id --для связи с дорогами2
    LEFT JOIN dic.fku ON fku.item_id = po.fku_item_id -- фку для параметра is_active
    WHERE (po.start_year <= {year} AND po.end_year >= {year}) --год
    AND (
    (po.is_correct_edit = TRUE 
    AND po.status_guid IN (
    '981b6a0a-791c-47cd-a32a-80204a27f6df', --Не рассмотрено
    'fcfae950-7582-4777-99ca-29b87ab931f0', --Отклонено
    'dcff8605-77e7-48f9-9b49-e03f005dc73d' --Принято к утверждению
    )) 
    OR 
    (po.is_correct_delete = TRUE 
    AND po.status_guid IN (
    '981b6a0a-791c-47cd-a32a-80204a27f6df', --Не рассмотрено
    'fcfae950-7582-4777-99ca-29b87ab931f0') --Отклонено
    )
    --OR (po.is_correct_add = TRUE
    --AND po.status_guid = 'dcff8605-77e7-48f9-9b49-e03f005dc73d')
    ) --Принято к утверждению
    AND p.status = 'Утверждена' --ПР утверждена
    AND dic.fku.is_active = TRUE -- только активные фку
    {list_parameters['fku_switch']} = '{list_parameters['fku']}' --(фильтр по фку) ФКУ Упрдор «Северо-Запад»
    {list_parameters['district_switch']}AND d.item_id = '{list_parameters['district']}' --(фильтр по фед округам) Северо-Западный федеральный округ
    {list_parameters['region_switch']}AND r.item_id = '{list_parameters['region']}' --(фильтр по региону) Ленинградская область
    {list_parameters['program_type_switch']}AND po.program_type_item_id = '{list_parameters['program_type']}' --(фильтр по ПР) Содержание  автомобильных дорог
    {list_parameters['road_switch']}AND ((po.item_id = co.object_id) AND (COALESCE (rs.highway_section_item_id, '00000000-0000-0000-0000-000000000000') = '{list_parameters['road']}')) --(фильтр по дороге) Санкт-Петербург - Псков - Пустошка- Невель - граница с Республикой Белоруссия """
    filter_pom = f"""SELECT DISTINCT (pom.item_id)
    FROM program_object_maintenance pom
    LEFT JOIN program p ON p.item_id=pom.program_item_id --программы работ
    LEFT JOIN dic.region r ON r.item_id=pom.region_item_id --регионы
    LEFT JOIN dic.district d ON d.item_id=r.district_item_id --фед округа
    LEFT JOIN dic.road_section rs ON rs.item_id=pom.road_section_item_id --для связи с дорогами для содержания
    LEFT JOIN dic.fku ON fku.item_id = pom.fku_item_id -- фку для параметра is_active
    WHERE (pom.start_year <= {year} AND pom.end_year >= {year}) --годWHERE (pom.start_year = {year}) --год
    AND pom.status_guid = 'ff0f2572-ae63-4d02-bc83-f5e8267792b6'--Утверждено
    AND p.status = 'Утверждена' --ПР утверждена
    AND dic.fku.is_active = TRUE -- только активные фку
    {list_parameters['fku_switch']}AND pom.fku_item_id = '{list_parameters['fku']}' --(фильтр по фку) ФКУ Упрдор «Северо-Запад»
    {list_parameters['district_switch']}AND d.item_id = '{list_parameters['district']}' --(фильтр по фед округам) Северо-Западный федеральный округ
    {list_parameters['region_switch']}AND r.item_id = '{list_parameters['region']}' --(фильтр по региону) Ленинградская область
    {list_parameters['program_type_switch']}AND pom.program_type_item_id = '{list_parameters['program_type']}' --(фильтр по ПР) Содержание  автомобильных дорог
    {list_parameters['road_switch']}AND COALESCE(rs.highway_section_item_id, '00000000-0000-0000-0000-000000000000') = '{list_parameters['road']}' --(фильтр по дороге) Санкт-Петербург - Псков - Пустошка- Невель - граница с Республикой Белоруссия  """
    filter_pom_correct = f"""SELECT DISTINCT (pom.item_id)
                    FROM program_object_maintenance pom
                    LEFT JOIN program p ON p.item_id=pom.program_item_id --программы работ
                    LEFT JOIN dic.region r ON r.item_id=pom.region_item_id --регионы
                    LEFT JOIN dic.district d ON d.item_id=r.district_item_id --фед округа
                    LEFT JOIN dic.road_section rs ON rs.item_id=pom.road_section_item_id --для связи с дорогами для содержания
                    LEFT JOIN dic.fku ON fku.item_id = pom.fku_item_id -- фку для параметра is_active
                    WHERE (pom.start_year <= {year} AND pom.end_year >= {year}) --годQ
                    AND (
                    (pom.is_correct_edit = TRUE
                    AND pom.status_guid IN (
                    '451ed9e9-833d-48b3-acf7-3061c5f70b16', --Не рассмотрено
                    '40e868a8-0352-42eb-ab0a-72540dece44f', --Отклонено
                    '069fc5cd-42ab-4d37-8e54-451f91129713' --Принято к утверждению
                    ))
                    OR (pom.is_correct_delete = TRUE
                    AND pom.status_guid IN (
                    '451ed9e9-833d-48b3-acf7-3061c5f70b16', --Не рассмотрено
                    '40e868a8-0352-42eb-ab0a-72540dece44f' --Отклонено
                    )) 
                    --OR (pom.is_correct_add = TRUE 
                    --AND pom.status_guid = '069fc5cd-42ab-4d37-8e54-451f91129713')
                    )
                    AND p.status = 'Утверждена' --ПР утверждена
                    AND dic.fku.is_active = TRUE -- только активные фку
                    {list_parameters['fku_switch']}AND pom.fku_item_id = '{list_parameters['fku']}' --(фильтр по фку) ФКУ Упрдор «Северо-Запад»
                    {list_parameters['district_switch']}AND d.item_id = '{list_parameters['district']}' --(фильтр по фед округам) Северо-Западный федеральный округ
                    {list_parameters['region_switch']}AND r.item_id = '{list_parameters['region']}' --(фильтр по региону) Ленинградская область
                    {list_parameters['program_type_switch']}AND pom.program_type_item_id = '{list_parameters['program_type']}' --(фильтр по ПР) Содержание  автомобильных дорог
                    {list_parameters['road_switch']}AND COALESCE(rs.highway_section_item_id, '00000000-0000-0000-0000-000000000000') = '{list_parameters['road']}' --(фильтр по дороге) Санкт-Петербург - Псков - Пустошка- Невель - граница с Республикой Белоруссия  """
    filter_data = {"filter_po": filter_po, "filter_po_correct": filter_po_correct, 'filter_pom': filter_pom, 'filter_pom_correct': filter_pom_correct}
    return filter_data


def execute_sql_not_started(year, cursor, list_parameters):
    filter_data = filter_model(year, list_parameters)
    cursor.execute(
                    f"""SELECT COUNT(*), coalesce(SUM(VALUE), NULL, 0) FROM
                    ((SELECT po.item_id AS program_obj,  SUM(job_amount.value) AS VALUE,
                    (CASE WHEN po.item_id IS NOT NULL THEN 0
                            ELSE 1
                        END) AS is_pom
                    from program_object po
                    left JOIN job_amount
                    ON po.item_id = job_amount.object_item_id
                    WHERE po.item_id in
                    (
                    {filter_data["filter_po"]}
                    )
                    AND po.item_id  in
                    (SELECT program_object.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object 
                    ON program_object.item_id = job_amount_proxy.object_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object.start_year <= {year} AND program_object.end_year >= {year}) --год
                    GROUP BY program_object.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0)
                    AND job_amount.year = {year}
                    GROUP BY po.item_id, program_obj, is_pom)
                    UNION
                    SELECT just_table.program_obj, snapshot_table.VALUE, snapshot_table.is_pom
                    from
                    (SELECT po.item_id AS program_obj,  SUM(job_amount_snapshot.value) AS value,
                    
                    (CASE WHEN po.item_id IS NOT NULL THEN 0
                            ELSE 1
                        END) AS is_pom
                    from program_object po                   
                    left JOIN job_amount_snapshot
                    ON job_amount_snapshot.object_item_id = po.item_id
                    WHERE po.item_id in
                    (
                    {filter_data["filter_po_correct"]}
                    )
                    AND po.item_id  in
                    (SELECT program_object.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object 
                    ON program_object.item_id = job_amount_proxy.object_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object.start_year <= {year} AND program_object.end_year >= {year}) --год
                    GROUP BY program_object.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0)
                    AND job_amount_snapshot.year = {year}
                    GROUP BY program_obj, is_pom) AS snapshot_table
                    RIGHT JOIN
                    
                    (SELECT po.item_id AS program_obj
                    
                    from program_object po
                    
                    left JOIN job_amount
                    ON job_amount.object_item_id = po.item_id
                    
                    WHERE po.item_id in
                    (
                    {filter_data["filter_po_correct"]}
                    )
                    
                    AND po.item_id  in
                    
                    (SELECT program_object.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object 
                    ON program_object.item_id = job_amount_proxy.object_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object.start_year <= {year} AND program_object.end_year >= {year}) --год
                    GROUP BY program_object.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0)
                    
                    AND job_amount.year = {year}
                    GROUP BY program_obj) AS just_table
                    ON  snapshot_table.program_obj = just_table.program_obj
                    
                    
                    
                    
                    
                    UNION
                    
                    
                    (SELECT pom.item_id, SUM(job_amount.value) AS Job_amount_sum,
                    (CASE WHEN job_amount.object_maintenance_item_id IS NOT NULL THEN 1
                            ELSE 0
                        END) AS is_pom
                    
                    from program_object_maintenance pom
                    
                    left JOIN job_amount
                    ON pom.item_id = job_amount.object_maintenance_item_id
                    
                    
                    WHERE pom.item_id IN
                    
                    
                    
                    (
                    {filter_data["filter_pom"]}
                    )
                    AND pom.item_id  IN 
                    
                    (SELECT program_object_maintenance.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object_maintenance 
                    ON program_object_maintenance.item_id = job_amount_proxy.object_maintenance_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object_maintenance.start_year <= {year} AND program_object_maintenance.end_year >= {year}) --год
                    GROUP BY program_object_maintenance.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0)
                        
                    AND job_amount.year = {year}
                    GROUP BY pom.item_id, is_pom)
                    
                    
                    
                    UNION
                    
                    
                    
                    SELECT just_table.pom_id, snapshot_table.job_amount_snapshot_sum, snapshot_table.is_pom
                    from
                    (SELECT pom.item_id AS pom_id,  SUM(job_amount_snapshot.value) AS job_amount_snapshot_sum,
                    (CASE WHEN job_amount_snapshot.object_maintenance_item_id IS NOT NULL THEN 1
                            ELSE 0
                        END) AS is_pom
                    
                    from program_object_maintenance pom
                    
                    
                    left JOIN job_amount_snapshot
                    ON job_amount_snapshot.object_maintenance_item_id = pom.item_id
                    
                    
                    WHERE pom.item_id IN
                    (
                    {filter_data["filter_pom_correct"]}
                    )
                    
                    AND pom.item_id  IN 
                    
                    (SELECT program_object_maintenance.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object_maintenance 
                    ON program_object_maintenance.item_id = job_amount_proxy.object_maintenance_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object_maintenance.start_year <= {year} AND program_object_maintenance.end_year >= {year}) --год
                    GROUP BY program_object_maintenance.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0) 
                        
                    --and lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586' -- ид госконтракт заключен
                    
                    AND job_amount_snapshot.year = {year}
                    GROUP BY pom.item_id, is_pom) AS snapshot_table
                    
                    RIGHT JOIN 
                    
                    
                    
                    (SELECT pom.item_id AS pom_id
                    
                    from program_object_maintenance pom
                    left JOIN job_amount
                    ON job_amount.object_maintenance_item_id = pom.item_id
                    
                    
                    WHERE pom.item_id IN
                    (
                    {filter_data["filter_pom_correct"]}
                    )
                    
                    AND pom.item_id  IN 
                    
                    (SELECT program_object_maintenance.item_id AS progr
                    FROM job_amount_proxy
                    left join lot
                    on job_amount_proxy.lot_item_id = lot.item_id
                    LEFT join calendar_schedule  
                    on calendar_schedule.job_amount_proxy_item_id = job_amount_proxy.item_id
                    LEFT join program_object_maintenance 
                    ON program_object_maintenance.item_id = job_amount_proxy.object_maintenance_item_id
                    where lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586'
                    and (program_object_maintenance.start_year <= {year} AND program_object_maintenance.end_year >= {year}) --год
                    GROUP BY program_object_maintenance.item_id, job_amount_proxy.item_id
                    HAVING SUM((
                    CASE WHEN calendar_schedule.item_id IS NULL THEN 0
                            ELSE 1
                        END))  = 0)
                        
                    --and lot.procedure_item_id = '9654a4ba-1823-4c8c-8c09-c5703a3d3586' -- ид госконтракт заключен
                    
                    AND job_amount.year = {year}
                    GROUP BY pom.item_id) AS just_table
                    
                    ON just_table.pom_id = snapshot_table.pom_id) AS a 
                    """
                    )
    row = cursor.fetchall()
    count_not_started = int(row[0][0])
    sum_not_started = int(row[0][1])
    return count_not_started, sum_not_started


def execute_sql_total_reserve(year, cursor, list_parameters):
    filter_data = filter_model(year, list_parameters)
    cursor.execute(
                    f"""     SELECT COALESCE(SUM(PUBLIC.limits.approved), 0) 
                    - (SELECT COALESCE(SUM(public.job_amount.value), 0)
                    FROM public.job_amount
                    left JOIN dic.article
                    ON dic.article.item_id = job_amount.article_item_id
                    WHERE job_amount.year = {year}
                    AND job_amount.object_item_id IN
                    (
                    {filter_data['filter_po']}
                    )) 
                    - (SELECT COALESCE(SUM(public.job_amount.value), 0)
                    FROM public.job_amount
                    left JOIN dic.article
                    ON dic.article.item_id = job_amount.article_item_id
                    WHERE job_amount.year = {year}
                    AND job_amount.object_maintenance_item_id IN
                    (
                      {filter_data['filter_pom']}                  
                                        ))
                    - (SELECT COALESCE(SUM(public.job_amount_snapshot.value), 0)
                    FROM public.job_amount_snapshot
                    left JOIN dic.article
                    ON dic.article.item_id = job_amount_snapshot.article_item_id
                    WHERE job_amount_snapshot.year = {year}
                    AND job_amount_snapshot.object_maintenance_item_id IN
                    (
                    {filter_data['filter_pom_correct']}
                    ))
                    - (SELECT COALESCE(SUM(public.job_amount_snapshot.value), 0)
                    FROM public.job_amount_snapshot
                    left JOIN dic.article
                    ON dic.article.item_id = job_amount_snapshot.article_item_id
                    WHERE job_amount_snapshot.year = {year}
                    AND job_amount_snapshot.object_item_id IN
                    (
                    {filter_data['filter_po_correct']}
                    ))
                    - ( SELECT COALESCE(SUM(unallocated_fund_article.value), 0)
                    FROM unallocated_fund
                    JOIN dic.years
                    ON dic.years.item_id = unallocated_fund.year_item_id
                    JOIN unallocated_fund_article
                    on unallocated_fund_article.fund_item_id = unallocated_fund.item_id
                    JOIN dic.article
                    ON dic.article.item_id = unallocated_fund_article.article_item_id
                    LEFT JOIN dic.fku
                    ON unallocated_fund.fku_item_id = dic.fku.item_id
                    WHERE dic.years.year = {year}
                    AND unallocated_fund.status_item_id = '01ded5b8-7897-425a-9943-954be00b79ec' -- утверждено
                    {list_parameters['program_type_switch']}AND unallocated_fund.program_type_item_id = '{list_parameters['program_type']}'
                    {list_parameters['fku_switch']}AND unallocated_fund.fku_item_id = '{list_parameters['fku_switch']}'
                    ----AND dic.article.name = '310'-- С Т А Т Ь Я
                    --AND EXISTS (SELECT 1 FROM dic.road_section WHERE dic.fku.item_id = dic.road_section.fku_item_id AND dic.road_section.highway_section_item_id = '{list_parameters['road']}')
                    --AND EXISTS (SELECT 1 FROM dic.road_section WHERE dic.fku.item_id = dic.road_section.fku_item_id AND dic.road_section.region_item_id = 'bbbbca6e-685c-40a2-8e31-3e2d7a182ceb')
                    )
                    FROM PUBLIC.limits
                    JOIN dic.years
                    ON limits.year_item_id = dic.years.item_id
                    JOIN dic.article
                    ON dic.article.item_id = limits.article_item_id
                    WHERE dic.years.year = {year}  
                    {list_parameters['program_type_switch']}AND limits.program_type_item_id = '{list_parameters['program_type']}'"""
                    )
    row = cursor.fetchone()
    total_reserve = int(row[0])
    return total_reserve
