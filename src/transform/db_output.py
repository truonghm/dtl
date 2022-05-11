movie_features_query = """
with mov as (
    select 
        movies.*,
        cast(strftime('%Y', date(opening_date)) as int) opening_year, 
        cast(strftime('%Y', date(release_date)) as int) release_year 
    from movies
),

new_cpi as (
    select *, (select avg_annual_cpi from cpi where year = 2022) current_cpi
    from cpi
    ),

af_filtered as (
    select distinct a.*
    from actor_filmo a
    left join actor_movie b on a.crew_url = b.actor_url
    left join mov on b.movie_id = mov.movie_id
    where 1=1
    and a.movie_id not in (select movie_id from movies)
    and a.year < mov.release_year
),

af_adj as (
    select 
        af.*,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from af_filtered af
    left join new_cpi on af.year = new_cpi.year
    where 1=1
    and is_star = 'True'
),

df_filtered as (
    select distinct a.*
    from director_filmo a
    left join director_movie b on a.crew_url = b.director_url
    left join mov on b.movie_id = mov.movie_id
    where 1=1
    and a.movie_id not in (select movie_id from movies)
    and a.year < mov.release_year
),

df_adj as (
    select 
        df.*,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from df_filtered df
    left join new_cpi on df.year = new_cpi.year
    where 1=1
),

wf_filtered as (
    select distinct a.*
    from writer_filmo a
    left join writer_movie b on a.crew_url = b.writer_url
    left join mov on b.movie_id = mov.movie_id
    where 1=1
    and a.movie_id not in (select movie_id from movies)
    and a.year < mov.release_year
),

wf_adj as (
    select 
        wf.*,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from wf_filtered wf
    left join new_cpi on wf.year = new_cpi.year
    where movie_id not in (select movie_id from movies)
),

actor_profile as (
    select 
        af.crew_url actor_id, 
        a.name,
        -- avg(rating) avg_actor_movie_rating,
        -- avg(rating_count) avg_actor_movie_rating_count,
        -- avg(revenue_world_adj) avg_actor_movie_revenue_world_adj,

        -- avg(case when is_star = 'True' then rating end) avg_star_actor_movie_rating,
        -- avg(case when is_star = 'True' then rating_count end) avg_star_actor_movie_rating_count,
        -- avg(case when is_star = 'True' then revenue_world_adj end) avg_star_actor_movie_revenue_world_adj,

        -- max(case when is_star = 'True' then rating end) max_star_actor_movie_rating,
        -- max(case when is_star = 'True' then rating_count end) max_star_actor_movie_rating_count,
        -- max(case when is_star = 'True' then revenue_world_adj end) max_star_actor_movie_revenue_world_adj,

        -- min(case when is_star = 'True' then rating end) min_star_actor_movie_rating,
        -- min(case when is_star = 'True' then rating_count end) min_star_actor_movie_rating_count,
        -- min(case when is_star = 'True' then revenue_world_adj end) min_star_actor_movie_revenue_world_adj,

        -- avg(case when is_star = 'False' then rating end) avg_non_star_actor_movie_rating,
        -- avg(case when is_star = 'False' then rating_count end) avg_non_star_actor_movie_rating_count,
        -- avg(case when is_star = 'False' then revenue_world_adj end) avg_non_star_actor_movie_revenue_world_adj,

        sum(rating) sum_star_actor_movie_rating,
        sum(rating_count) sum_star_actor_movie_rating_count,
        sum(revenue_world_adj) sum_star_actor_movie_revenue_world_adj,

        count(crew_url) cnt_star_actor_movie

    from af_adj af
    left join actors a on af.crew_url = a.actor_id
    group by 
        af.crew_url, 
        a.name
),

director_profile as (
    select 
        df.crew_url director_id, 
        d.name,
        avg(rating) avg_director_movie_rating,
        avg(rating_count) avg_director_movie_rating_count,
        avg(revenue_world_adj) avg_director_movie_revenue_world_adj,

        max(rating) max_director_movie_rating,
        max(rating_count) max_director_movie_rating_count,
        max(revenue_world_adj) max_director_movie_revenue_world_adj,

        min(rating) min_director_movie_rating,
        min(rating_count) min_director_movie_rating_count,
        min(revenue_world_adj) min_director_movie_revenue_world_adj,

        sum(rating) sum_director_movie_rating,
        sum(rating_count) sum_director_movie_rating_count,
        sum(revenue_world_adj) sum_director_movie_revenue_world_adj,

        count(crew_url) cnt_director_movie
    from df_adj df
    left join directors d on df.crew_url = d.director_id
    group by 
        df.crew_url, 
        d.name
),

writer_profile as (
    select 
        wf.crew_url writer_id, 
        w.name,
        avg(rating) avg_writer_movie_rating,
        avg(rating_count) avg_writer_movie_rating_count,
        avg(revenue_world_adj) avg_writer_movie_revenue_world_adj,

        max(rating) max_writer_movie_rating,
        max(rating_count) max_writer_movie_rating_count,
        max(revenue_world_adj) max_writer_movie_revenue_world_adj,

        min(rating) min_writer_movie_rating,
        min(rating_count) min_writer_movie_rating_count,
        min(revenue_world_adj) min_writer_movie_revenue_world_adj,

        sum(rating) sum_writer_movie_rating,
        sum(rating_count) sum_writer_movie_rating_count,
        sum(revenue_world_adj) sum_writer_movie_revenue_world_adj,

        count(crew_url) cnt_writer_movie 
    from wf_adj wf
    left join writers w on wf.crew_url = w.writer_id
    group by 
        wf.crew_url, 
        w.name
),

movie_af as (
    select 
        am.movie_id,
        sum(sum_star_actor_movie_rating) / sum(cnt_star_actor_movie) avg_movie_rating_by_star_actor,
        sum(sum_star_actor_movie_rating_count) / sum(cnt_star_actor_movie) avg_movie_rating_count_by_star_actor,
        sum(sum_star_actor_movie_revenue_world_adj) / sum(cnt_star_actor_movie) avg_movie_revenue_world_adj_by_star_actor
    from actor_movie am
    left join actor_profile ap on am.actor_url = ap.actor_id
    where is_star = 'True'
    group by am.movie_id
),

movie_df as (
    select 
        dm.movie_id,
        sum(sum_director_movie_rating) / sum(cnt_director_movie) avg_movie_rating_by_director,
        sum(sum_director_movie_rating_count) / sum(cnt_director_movie) avg_movie_rating_count_by_director,
        sum(sum_director_movie_revenue_world_adj) / sum(cnt_director_movie) avg_movie_revenue_world_adj_by_director
    from director_movie dm
    left join director_profile dp on dm.director_url = dp.director_id
    group by dm.movie_id
),

movie_wf as (
    select 
        wm.movie_id,
        sum(sum_writer_movie_rating) / sum(cnt_writer_movie) avg_movie_rating_by_writer,
        sum(sum_writer_movie_rating_count) / sum(cnt_writer_movie) avg_movie_rating_count_by_writer,
        sum(sum_writer_movie_revenue_world_adj) / sum(cnt_writer_movie) avg_movie_revenue_world_adj_by_writer
    from writer_movie wm
    left join writer_profile wp on wm.writer_url = wp.writer_id
    group by wm.movie_id
),

total_rating as (
select 
    movie_id,
    sum(vote_count) rating_count
    from rating_dist
    group by movie_id
),

mov_adj as (
    select 
        mov.movie_rank,
        mov.movie_id,
        mov.name,
        mov.popularity,
        mov.rating,
        tr.rating_count,
        -- mov.user_review_count,
        mov.critic_review_count,
        mov.runtime,
        mov.release_date,
        mov.release_year,

        ma.avg_movie_rating_by_star_actor,
        ma.avg_movie_rating_count_by_star_actor,
        ma.avg_movie_revenue_world_adj_by_star_actor / 10E6 avg_movie_revenue_world_adj_by_star_actor,

        md.avg_movie_rating_by_director,
        md.avg_movie_rating_count_by_director,
        md.avg_movie_revenue_world_adj_by_director / 10E6 avg_movie_revenue_world_adj_by_director,

        mw.avg_movie_rating_by_writer,
        mw.avg_movie_rating_count_by_writer,
        mw.avg_movie_revenue_world_adj_by_writer / 10E6 avg_movie_revenue_world_adj_by_writer,

        revenue_world * avg_annual_cpi / current_cpi / 10E6 revenue_world_adj,
        revenue_usa * avg_annual_cpi / current_cpi / 10E6 revenue_usa_adj,
        budget * avg_annual_cpi / current_cpi / 10E6 budget_adj

    from mov
    left join new_cpi on mov.release_year = new_cpi.year
    left join movie_af ma on mov.movie_id = ma.movie_id
    left join movie_df md on mov.movie_id = md.movie_id
    left join movie_wf mw on mov.movie_id = mw.movie_id
    left join total_rating tr on mov.movie_id =tr.movie_id
)

select * from mov_adj
"""

genre_query = """
with mov as (
    select 
        movies.*,
        cast(strftime('%Y', date(opening_date)) as int) opening_year, 
        cast(strftime('%Y', date(release_date)) as int) release_year 
    from movies
),

mov_adj as (
    select 
        mov.*,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj,
        revenue_usa * avg_annual_cpi / current_cpi revenue_usa_adj,
        revenue_usa_opening * avg_annual_cpi / current_cpi revenue_usa_opening_adj,
        budget * avg_annual_cpi / current_cpi budget_adj
    from mov
    left join new_cpi on mov.release_year = new_cpi.year
),

base as (select distinct 
    gm.genre, 
    m.* 
from genre_movie gm
inner join mov_adj m
on gm.movie_id = m.movie_id
),



new_cpi as (
    select *, (select avg_annual_cpi from cpi where year = 2022) current_cpi
    from cpi
    ),


total_rating as (
select 
    movie_id,
    sum(vote_count) rating_count
    from rating_dist
    group by movie_id
)

select 
    genre,
    movie_rank,
    base.movie_id,
    name,
    popularity,
    rating,
    tr.rating_count,
    critic_review_count,
    budget_adj / 10E6 budget_adj,
    revenue_usa_adj / 10E6 revenue_usa_adj,
    revenue_usa_opening_adj / 10E6 revenue_usa_opening_adj,
    revenue_world_adj / 10E6 revenue_world_adj,
    runtime,
    opening_date,
    release_date
from base
left join total_rating tr
on base.movie_id =tr.movie_id
"""