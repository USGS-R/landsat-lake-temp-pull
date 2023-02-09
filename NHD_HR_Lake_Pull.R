## Mess around with tyler kings stuff
library(tidyverse)
library(sf)
library(mapview)
library(feather)
mapviewOptions(fgb=FALSE)

tk_resolvable <- vroom::vroom('/Users/stopp/Downloads/NHDWaterbody_resolvable.csv')


### Read in the exports from EE and do a little munging
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

#write_feather(dp_national, 'data/out/nhd_hr_1ha_deepest_point_noTX.feather')
dp_national <- read_feather('data/out/nhd_hr_1ha_deepest_point_noTX.feather')
tk_resolvable <- vroom::vroom('/Users/stopp/Downloads/NHDWaterbody_resolvable.csv')


match_ups <- dp_national %>%
  inner_join(.,tk_resolvable, by = c('permanent'='permanent_identifier'))

rm(dp_national, tk_resolvable)

match_ups <- match_ups %>% mutate(resolvable = ifelse(resolvable == 1, 'Konrad resolvable', 'Konrad not resolvable'))

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



tk30_res <- tk_resolvable %>% filter(distance < 30)
tk30_not_res <- tk_non_resolvable %>% filter(distance >30)

ggplot(tk30_res, aes(x = areasqkm)) +
  geom_histogram()

ggplot(tk30_not_res, aes(x= areasqkm)) + 
  geom_histogram() +
  scale_x_log10()


tk30_res %>% slice_sample(prop=.2) %>% 
  st_as_sf(., coords=c('long','lat'), crs=4326) %>%
  mapview(zcol = 'distance')
  
