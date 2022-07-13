## Mess around with tyler kings stuff
#resolvable <- vroom::vroom('/Users/stopp/Downloads/NHDWaterbody_resolvable.csv')
library(tidyverse)
library(dataRetrieval)
library(stringr)
library(sf)
library(httr)
library(sbtools)
sf::sf_use_s2(FALSE)
library(mapview)
mapviewOptions(fgb=FALSE)
#library(smoothr)
# 
# ## Check the layers
# st_layers('/Users/stopp/Downloads/NHD_H_National_GDB/NHD_H_National_GDB.gdb')
# 
# ## Pull the first few waterbodies to check the fields
# check_fields <- st_read('/Users/stopp/Downloads/NHD_H_National_GDB/NHD_H_National_GDB.gdb',
#                   query = 'SELECT * FROM "NHDWaterbody" WHERE FID < 5', layer = 'NHDWaterbody')
# 
# ## Set up a query to just bring in the fields we want for lakes and reservoirs (get rid of swamps, etc)
# lakes <- st_read('/Users/stopp/Downloads/NHD_H_National_GDB/NHD_H_National_GDB.gdb',
#                  query = 'SELECT PERMANENT_IDENTIFIER, GNIS_NAME, AREASQKM, ELEVATION, FTYPE, FCODE FROM "NHDWaterbody" WHERE FTYPE = 361 OR FTYPE = 436 OR FTYPE = 390', layer = 'NHDWaterbody') #%>%
# 
# #### Simplify everything in chunks 
# step = 100000
# for(i in seq(0,nrow(lakes),step)){
#   
#   end <- min(i+step, nrow(lakes))
#   # For re-runs, skip what's been done
#   files <- list.files('data/out/tmp_waterbody_chunks')
#   if(T %in% grepl(sprintf('%i_%i.shp',i+1,end),files)) next
#   
#   ## Chunk and simplify
#   lakes_out <- lakes[(i+1):end,] %>%
#     st_cast(., "MULTIPOLYGON")%>%
#     # bad Idaho site!
#     filter(PERMANENT_IDENTIFIER != '{5BEDE13F-C94B-4501-B979-E00C29EA374B}') %>%
#     sf::st_transform(crs = 4326) %>%
#     sf::st_zm() %>%
#     sf::st_buffer(dist = 0) %>%
#     sf::st_simplify(preserveTopology = TRUE, dTolerance = 0.0001)
#   
#   st_write(lakes_out,sprintf('data/out/tmp_waterbody_chunks/%i_%i.shp',i+1,i+step))
# }
# 
# rm(lakes)
# 
# chunks <- list.files('data/out/tmp_waterbody_chunks', pattern='*.shp',full.names = T)
# lakes_simplified <- purrr::map_dfr(chunks, st_read)
# 
# object.size(lakes_simplified)
# 
# length(unique(lakes_simplified$PERMANE))
# 
# mapview(sample_n(lakes_simplified,5000))
# 
# st_write(lakes_simplified, 'data/out/NHD_waterbodies/NHD_waterbodies_simplified.shp')

tk_resolvable <- vroom::vroom('/Users/stopp/Downloads/NHDWaterbody_resolvable.csv')

files <- list.files('../../../My Drive/EE_DP_Exports', full.names = T)

options(digits=15)
dp_national <- read_csv(files[1]) %>%
  mutate(coords = map(.geo, ~as.numeric(unlist(regmatches(.x,gregexpr("-?[[:digit:]]+\\.*-?[[:digit:]]*",.x))))),
         long = map_dbl(coords, 1),
         lat = map_dbl(coords,2),
         distance = round(distance,3),
         areasqkm = round(areasqkm, 4)) %>%
  select(-c(coords, .geo,`system:index`))

for(i in files[2:length(files)]){
  state <- read_csv(i,show_col_types = F) %>%
    mutate(coords = map(.geo, ~as.numeric(unlist(regmatches(.x,gregexpr("-?[[:digit:]]+\\.*-?[[:digit:]]*",.x))))),
           long = map_dbl(coords, 1),
           lat = map_dbl(coords,2),
           distance = round(distance,3),
           areasqkm = round(areasqkm, 4)) %>%
    select(-c(coords, .geo,`system:index`))
  
  dp_national <- dp_national %>%
    bind_rows(state) %>%
    distinct(permanent,.keep_all=T)
}

dp_national <- dp_national %>%
  select(areasqkm:lat)

library(feather)
write_feather(dp_national, 'data/out/nhd_hr_1ha_deepest_point_noTX.feather')


match_ups <- dp_national %>%
  inner_join(.,tk_resolvable, by = c('permanent'='permanent_identifier'))
rm(dp_national, tk_resolvable)

match_ups <- match_ups %>% mutate(resolvable = ifelse(resolvable == 'resolvable', 'Konrad resolvable', 'Konrad not resolvable'))

thresholds <- tibble(distance = c(20,30,40,50,60,70,80,90))

tk_resolvable <- match_ups %>% filter(resolvable == 'Konrad resolvable')

thresholds$counts <- colSums(outer(tk_resolvable$distance, setNames(thresholds$distance, thresholds$distance), "<"))
thresholds$labels <- paste0(thresholds$distance,' meters (n=',thresholds$counts,')')

tk_resolvable %>%
  ggplot(.,aes(x=distance)) + stat_bin(aes(y=cumsum(..count..)),geom="step") +
  geom_vline(data= thresholds, aes(xintercept=distance, color = labels)) +
  scale_color_viridis_d() +
  scale_x_log10() +
  theme_bw() +
  facet_wrap(~resolvable, scales = 'free') +
  labs(x = 'Maximum Distance from Shore (Meters)',
       y= 'Cumulative Lake Count',
       color='Res. Threshold (n disagreement)')


thresholds <- tibble(distance = c(20,30,40,50,60,70,80,90))

tk_non_resolvable <- match_ups %>% filter(resolvable == 'Konrad not resolvable')

thresholds$counts <- colSums(outer(tk_non_resolvable$distance, setNames(thresholds$distance, thresholds$distance), ">"))
thresholds$labels <- paste0(thresholds$distance,' meters (n=',thresholds$counts,')')

tk_non_resolvable %>%
  ggplot(.,aes(x=distance)) + stat_bin(aes(y=cumsum(..count..)),geom="step") +
  geom_vline(data= thresholds, aes(xintercept=distance, color = labels)) +
  scale_color_viridis_d() +
  scale_x_log10() +
  theme_bw() +
  facet_wrap(~resolvable, scales = 'free') +
  labs(x = 'Maximum Distance from Shore (Meters)',
       y= 'Cumulative Lake Count',
       color='Res. Threshold (n disagreement)')


