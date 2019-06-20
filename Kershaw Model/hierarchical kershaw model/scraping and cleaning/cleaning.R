setwd("~/GitHub/Summer2019/Kershaw Model/hierarchical kershaw model/scraping and cleaning")
load('kershaw.Rdata')
packs <- c('tidyverse')
sapply(packs, require, character.only = T)

# remove all star games
gids <- unique(kershaw$gameday_link)
all.star1 <- grep("aasmlb_nasmlb_1", gids, value = T)
all.star2 <- grep("nasmlb_aasmlb_1", gids, value = T)
all.star <- c(all.star1, all.star2)
gids.keep <- gids[-which(gids %in% all.star)]
kershaw <- filter(kershaw, gameday_link  %in% gids.keep)
kershaw$gameday_link <- as.factor(kershaw$gameday_link)

# factor variables
kershaw$count <- as.factor(kershaw$count)
kershaw$type <- as.factor(kershaw$type)
kershaw$b_height <- factor(kershaw$b_height,levels(factor(kershaw$b_height))[c(3:6,1:2,7:14)])
kershaw$pitch_type <- as.factor(kershaw$pitch_type)
kershaw$gameday_link <- as.factor(kershaw$gameday_link)
game_ids <- unique(kershaw$gameday_link)

# remove at bats with NA, IN, UN
test <- summarize(group_by(kershaw, gameday_link), total = n())
kershaw.drop <- data.frame(matrix(0, 1, ncol(kershaw)))
names(kershaw.drop) <- names(kershaw)
for(i in 1:length(game_ids)){
  temp.df <- subset(kershaw, gameday_link == game_ids[i])
  drop <- unique(filter(temp.df, is.na(pitch_type) | pitch_type == "UN" | pitch_type == "IN")$num)
  if(rapportools::is.empty(drop)){
    removed.df <- temp.df
  } else{removed.df <- temp.df[-c(which(temp.df$num %in% drop)),]}
  kershaw.drop <- as.tbl(rbind(kershaw.drop, removed.df))
}
kershaw.drop <- kershaw.drop[-1,]
kershaw.drop$pitch_type <- factor(kershaw.drop$pitch_type)
kershaw <- kershaw.drop

# combine fastballs
kershaw$pitch_type <- factor(ifelse(kershaw$pitch_type == 'FT', 'FF', as.character(kershaw$pitch_type)))

# previous pitch variable
game_ids <- unique(kershaw$gameday_link)
prev_pitch <- NULL
for(i in 1:length(game_ids)){
  temp.dat <- subset(kershaw, kershaw$gameday_link == game_ids[i])
  temp.dat$num <- as.factor(temp.dat$num)
  at_bats <- unique(temp.dat$num)
  one_game <- NULL
  for(j in 1:length(at_bats)){
    temp.dat2 <- subset(temp.dat, num == at_bats[j])
    pitches <- as.character(temp.dat2$pitch_type)
    pitches <- c("None", pitches)
    pitches <- pitches[-length(pitches)]
    one_game <- c(one_game, pitches)
  }
  prev_pitch <- c(prev_pitch, one_game)
}
kershaw$prev_pitch <- as.factor(prev_pitch)
kershaw$prev_pitch <- relevel(kershaw$prev_pitch, ref = "None")

prev_pitch_type <- NULL
for(i in 1:length(game_ids)){
  temp.dat <- subset(kershaw, kershaw$gameday_link == game_ids[i])
  temp.dat$num <- as.factor(temp.dat$num)
  at_bats <- unique(temp.dat$num)
  one_game <- NULL
  for(j in 1:length(at_bats)){
    temp.dat2 <- subset(temp.dat, num == at_bats[j])
    pitches <- as.character(temp.dat2$type)
    pitches <- c("None", pitches)
    pitches <- pitches[-length(pitches)]
    one_game <- c(one_game, pitches)
  }
  prev_pitch_type <- c(prev_pitch_type, one_game)
}
kershaw$prev_pitch_type <- as.factor(prev_pitch_type)

# on base
onbase <- t(apply(kershaw[,43:45], 1, FUN = function(x) {!is.na(x)}))
kershaw$on.1b <- factor(as.numeric(onbase[,1]))
kershaw$on.2b <- factor(as.numeric(onbase[,2]))
kershaw$on.3b <- factor(as.numeric(onbase[,3]))

# pitch_count
kershaw <- kershaw %>%
  group_by(gameday_link) %>%
  mutate(pitch_count = row_number()) %>%
  ungroup()

save(kershaw, file = 'kershaw_clean.Rdata')
