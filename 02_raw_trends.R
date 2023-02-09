#### This script is an initial analysis of lake surface temperature trends
#### using temperatures from the Landsat.  ITS VERY PRELIMINARY and hacked together using
#### two different EE pulls.
library(tidyverse)
library(feather)
library(lubridate)
library(trend)
library(sf)
library(scales)

l7l8 <- read_feather('data/out/landsat_temps.feather')
l7l8 <- l7l8 %>%
  select(areakm, sat, date, distance, site_id, temp_ls, temp_ls_qa) %>%
  filter(sat=='LANDSAT_7')

ggplot(l7l8, aes(x=temp_ls)) + geom_histogram()

l8l9 <- read_csv('/Users/stopp/Documents/repos/landsat-lake-temp-pull/data/out/L8_L9_cb_temps_munged.csv')
l8l9 <- l8l9 %>%
  select(areakm, sat = SPACECRAFT_ID, date, distance, site_id, temp_ls = ls_temp, temp_ls_qa = ls_temp_qa) %>%
  mutate(sat=as.character(sat))

ggplot(l8l9, aes(x=temp_ls)) + geom_histogram()

ls_temps <- bind_rows(l7l8,l8l9) %>%
  mutate(site_id = factor(site_id))

rm(l7l8,l8l9)

seasonally_aggregated <- ls_temps %>%
  mutate(month = month(date),
         year = year(date),
         season = case_when((month %in% c(6,7,8)) ~ 'JJA',
                            (month %in% c(9,10,11)) ~ "SON",
                            (month %in% c(12,1,2)) ~ 'DJF',
                            (month %in% c(3,4,5)) ~'MAM'),
         season=factor(season)) %>%
  group_by(site_id, year, season, distance, areakm) %>%
  summarize(temp_ls = mean(temp_ls),
            count = n())

seasonally_aggregated <- seasonally_aggregated %>% filter(count > 1)

counts <- seasonally_aggregated %>%
  group_by(site_id, season) %>%
  mutate(count = n())

seasonally_aggregated <- seasonally_aggregated[counts$count > 20,]

trends <- seasonally_aggregated %>% 
  group_by(site_id, season) %>%
  arrange(year) %>%
  nest() %>%
  mutate(mk = purrr::map(data, ~sens.slope(.$temp_ls)),
         sen.slope = purrr::map_dbl(mk, 'estimates'),
         p.value = purrr::map_dbl(mk, 'p.value')) %>%
  select(site_id, season, sen.slope, p.value)

ggplot(trends, aes(x=sen.slope, fill = season)) + 
  geom_density(alpha=.4) +
  xlim(-.3,.4) +
  geom_vline(aes(xintercept=0), color = 'red') +
  scale_fill_viridis_d()

### Look at spatial trends
lakes_sf <- read_csv('data/in/lake_metadata_20211217.csv') %>%
  st_as_sf(coords = c('lake_lon_deg','lake_lat_deg'), crs = 4326) %>%
  select(site_id, elevation_m, area_m2) %>%
  st_transform(5070)

trends_sf <- lakes_sf %>% inner_join(trends)

usa <- maps::map('usa', plot = F) %>% st_as_sf() %>% st_transform(5070) 

grid <- st_make_grid(usa, cellsize = c(75000,75000), square = F) %>% st_as_sf() %>% mutate(ID = row_number())

grid_means <- grid %>% st_join(trends_sf %>% filter(abs(sen.slope) < .4) %>%st_transform(st_crs(grid)), left = F) %>%
  st_set_geometry(NULL) %>% group_by(ID, season) %>%
  summarize(mean_trend = mean(sen.slope))

means_sf <- grid %>% inner_join(grid_means)

ggplot() + 
  geom_sf(data = usa) +
  geom_sf(data = means_sf, aes(fill = mean_trend)) +
  scale_fill_gradient2(low = muted('blue'), high=muted('red'), 'Mean Trend (°C/yr)') +
  ggthemes::theme_map(base_size = 11) +
  theme(legend.position = 'bottom') +
  facet_wrap(~season) +
  ggtitle("Mean Trend in Lake Temperature Since 1999")

### Binned by Elevation
trends_sf %>%
  st_set_geometry(NULL) %>%
  mutate(elevation_bin = cut_interval(elevation_m, 5)) %>%
  ggplot(aes(x=elevation_bin,y = sen.slope,fill=elevation_bin)) +
  coord_cartesian(ylim=c(-.15,.25)) +
  geom_violin() +
  geom_boxplot(width=.2) +
  scale_fill_viridis_d(option='plasma', alpha=.3) +
  theme(axis.text.x = element_text(angle=45,hjust=1,vjust=1),
        legend.position = 'none') +
  labs(x = 'Elevation (m)', y = 'Trend (°C/yr)') 

### Binnned by lake size
trends_sf %>%
  st_set_geometry(NULL) %>%
  mutate(size_bin = cut_number(area_m2/1e6, 10)) %>%
  ggplot(aes(x=size_bin,y = sen.slope,fill=size_bin)) +
  coord_cartesian(ylim=c(-.15,.25)) +
  geom_violin() +
  geom_boxplot(width=.2) +
  scale_fill_viridis_d(option='plasma', alpha=.3) +
  theme(axis.text.x = element_text(angle=45,hjust=1,vjust=1),
        legend.position = 'none') +
  labs(x = 'Lake Size Bin (km2)', y = 'Trend (°C/yr)') 

seasonal_summary <- seasonally_aggregated %>%
  group_by(season, site_id) %>%
  mutate(temp_scaled = scale(temp_ls)) %>%
  ungroup() %>%
  group_by(season, year) %>%
  summarise(mean_temp= mean(temp_scaled),
            sd=sd(temp_scaled))

