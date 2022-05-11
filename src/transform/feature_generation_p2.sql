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
        gm.genre,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from af_filtered af
    left join new_cpi on af.year = new_cpi.year
    left join genre_movie gm on af.movie_id = gm.movie_id
    where 1=1
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
        gm.genre,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from df_filtered df
    left join new_cpi on df.year = new_cpi.year
    left join genre_movie gm on df.movie_id = gm.movie_id
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
        gm.genre,
        revenue_world * avg_annual_cpi / current_cpi revenue_world_adj
    from wf_filtered wf
    left join new_cpi on wf.year = new_cpi.year
    left join genre_movie gm on wf.movie_id = gm.movie_id
    where 1=1
),

actor_profile as (
    select 
        af.crew_url actor_id, 
        a.name,
        genre,
        sum(rating) sum_star_actor_movie_rating,
        sum(rating_count) sum_star_actor_movie_rating_count,
        sum(revenue_world_adj) sum_star_actor_movie_revenue_world_adj,

        count(crew_url) cnt_star_actor_movie

    from af_adj af
    left join actors a on af.crew_url = a.actor_id
    group by 
        af.crew_url, 
        a.name,
        genre
),

director_profile as (
    select 
        df.crew_url director_id, 
        d.name,
        genre,
        sum(rating) sum_director_movie_rating,
        sum(rating_count) sum_director_movie_rating_count,
        sum(revenue_world_adj) sum_director_movie_revenue_world_adj,

        count(crew_url) cnt_director_movie
    from df_adj df
    left join directors d on df.crew_url = d.director_id
    group by 
        df.crew_url, 
        d.name,
        genre
),

writer_profile as (
    select 
        wf.crew_url writer_id, 
        w.name,
        genre,
        sum(rating) sum_writer_movie_rating,
        sum(rating_count) sum_writer_movie_rating_count,
        sum(revenue_world_adj) sum_writer_movie_revenue_world_adj,

        count(crew_url) cnt_writer_movie 
    from wf_adj wf
    left join writers w on wf.crew_url = w.writer_id
    group by 
        wf.crew_url, 
        w.name,
        genre
),

movie_af as (
    select 
        am.movie_id,
        am.genre,
        sum(sum_star_actor_movie_rating) / sum(cnt_star_actor_movie) avg_movie_rating_by_star_actor,
        sum(sum_star_actor_movie_rating_count) / sum(cnt_star_actor_movie) avg_movie_rating_count_by_star_actor,
        sum(sum_star_actor_movie_revenue_world_adj) / sum(cnt_star_actor_movie) avg_movie_revenue_world_adj_by_star_actor
    from (
        select x.*, gm.genre from actor_movie x
        left join genre_movie gm on x.movie_id = gm.movie_id) am
    left join actor_profile ap on am.actor_url = ap.actor_id and am.genre = ap.genre
    where is_star = 'True'
    group by am.movie_id, am.genre
),

movie_df as (
    select 
        dm.movie_id,
        dm.genre,
        sum(sum_director_movie_rating) / sum(cnt_director_movie) avg_movie_rating_by_director,
        sum(sum_director_movie_rating_count) / sum(cnt_director_movie) avg_movie_rating_count_by_director,
        sum(sum_director_movie_revenue_world_adj) / sum(cnt_director_movie) avg_movie_revenue_world_adj_by_director
    from (
        select x.*, gm.genre from director_movie x
        left join genre_movie gm on x.movie_id = gm.movie_id) dm
    left join director_profile dp on dm.director_url = dp.director_id and dm.genre = dp.genre
    group by dm.movie_id, dm.genre
),

movie_wf as (
    select 
        wm.movie_id,
        wm.genre,
        sum(sum_writer_movie_rating) / sum(cnt_writer_movie) avg_movie_rating_by_writer,
        sum(sum_writer_movie_rating_count) / sum(cnt_writer_movie) avg_movie_rating_count_by_writer,
        sum(sum_writer_movie_revenue_world_adj) / sum(cnt_writer_movie) avg_movie_revenue_world_adj_by_writer
    from (
        select x.*, gm.genre from writer_movie x
        left join genre_movie gm on x.movie_id = gm.movie_id) wm
    left join writer_profile wp on wm.writer_url = wp.writer_id and wm.genre = wp.genre
    group by wm.movie_id, wm.genre
),

movie_af_final as (
    select 
        movie_id,
        avg(avg_movie_rating_by_star_actor) avg_movie_rating_by_star_actor_genre,
        avg(avg_movie_rating_count_by_star_actor)avg_movie_rating_count_by_star_actor_genre,
        avg(avg_movie_revenue_world_adj_by_star_actor) avg_movie_revenue_world_adj_by_star_actor_genre
    from movie_af
    group by movie_id
),


movie_df_final as (
    select 
        movie_id,
        avg(avg_movie_rating_by_director) avg_movie_rating_by_director_genre,
        avg(avg_movie_rating_count_by_director)avg_movie_rating_count_by_director_genre,
        avg(avg_movie_revenue_world_adj_by_director) avg_movie_revenue_world_adj_by_director_genre
    from movie_df
    group by movie_id
),

movie_wf_final as (
    select 
        movie_id,
        avg(avg_movie_rating_by_writer) avg_movie_rating_by_writer_genre,
        avg(avg_movie_rating_count_by_writer)avg_movie_rating_count_by_writer_genre,
        avg(avg_movie_revenue_world_adj_by_writer) avg_movie_revenue_world_adj_by_writer_genre
    from movie_wf
    group by movie_id
),

mov_adj as (
    select 
        mov.movie_id,
        ma.avg_movie_rating_by_star_actor_genre,
        ma.avg_movie_rating_count_by_star_actor_genre,
        ma.avg_movie_revenue_world_adj_by_star_actor_genre,

        md.avg_movie_rating_by_director_genre,
        md.avg_movie_rating_count_by_director_genre,
        md.avg_movie_revenue_world_adj_by_director_genre,

        mw.avg_movie_rating_by_writer_genre,
        mw.avg_movie_rating_count_by_writer_genre,
        mw.avg_movie_revenue_world_adj_by_writer_genre

    from mov
    left join movie_af_final ma on mov.movie_id = ma.movie_id
    left join movie_df_final md on mov.movie_id = md.movie_id
    left join movie_wf_final mw on mov.movie_id = mw.movie_id
)

select * from mov_adj