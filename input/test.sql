SELECT * FROM title t,movie_info mi,movie_companies mc WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.production_year>1990 AND mc.company_type_id=2;
SELECT * FROM movie_companies mc,title t,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id=112 AND mc.company_type_id=2;
SELECT * FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2005;

