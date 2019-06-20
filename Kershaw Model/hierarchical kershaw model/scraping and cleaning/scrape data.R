collapse_obs2 <- function(x) {
  val <- collapse_obs(x)
  if (is.list(val)) {
    return(val)
  } else {
    li <- list(val)
    names(li) <- unique(names(x))
    return(li)
  }
}

merged <- function(x, y, ...){
  dat <- merge(x=x, y=y, sort=FALSE, ...)
  dat[] <- lapply(dat, function(x) as.character(x))
  return(dat)
}

# Take a matrix and turn into data frame and turn relevant columns into numerics
format.table <- function(dat, name) {
  nums <- NULL
  switch(name,
         game = nums <- c("venue_id", "scheduled_innings", "away_team_id", "away_league_id", "home_team_id",
                          "home_league_id", "away_games_back_wildcard", "away_win",  "away_loss", "home_win",
                          "home_loss", "inning", "outs", "away_team_runs","home_team_runs", "away_team_hits",
                          "home_team_hits", "away_team_errors", "home_team_errors"),
         player = nums <- c("id", "num", "avg", "hr", "rbi", "bat_order", "wins", "losses", "era"),
         coach = nums <- c("id", "num"),
         umpire = nums <- "id",
         hip = nums <- c("x", "y", "batter", "pitcher", "inning"),
         action = nums <- c("b", "s", "o", "player", "pitch", "inning"),
         atbat = nums <- c("pitcher", "batter", "num", "b", "s", "o", "inning"),
         pitch = nums <- c("id", "x", "y", "start_speed", "end_speed", "sz_top", "sz_bot", "pfx_x", "pfx_z", "px",
                           "pz", "x0", "y0", "z0", "vx0", "vy0", "vz0", "ax", "ay", "az", "type_confidence",
                           "zone", "nasty", "spin_dir", "spin_rate", "inning", "num", "on_1b", "on_2b", "on_3b"),
         po = nums <- c("inning", "num"),
         runner = nums <- c("id", "inning", "num"))
  #For some reason, records are sometimes duplicated, remove them!
  dat <- data.frame(dat[!duplicated(dat),], stringsAsFactors=FALSE)
  nms <- names(dat)
  numz <- nums[nums %in% nms] #error handling (just in case one of the columns doesn't exist)
  for (i in numz) dat[, i] <- suppressWarnings(as.numeric(dat[, i]))
  if ("game" %in% name) {
    dat$url_scoreboard <- dat$url
    dat$url <- paste0(gsub("miniscoreboard.xml", "", dat$url), "gid_", dat$gameday_link, "/inning/inning_all.xml")
    # These fields only show up for suspended games...I don't think they're worth tracking...
    dat <- dat[, !names(dat) %in% c("runner_on_base_status", "runner_on_1b")]
  } else { #create a 'gameday_link' column for easier linking of tables
    if (length(grep("^url$", names(dat)))) dat$gameday_link <- sub("/.*", "", sub(".*gid", "gid", dat$url))
  }
  return(dat)
}

# Add columns with relevant pitch count to the 'pitch' table.
# @param dat 'pitch' matrix/df
# @return returns the original matrix/df with the proper pitch count column appended.
appendPitchCount <- function(dat) {
  if (any(!c("type", "gameday_link", "num") %in% colnames(dat))){
    warning("Count column couldn't be created")
    return(dat)
  }
  balls <- as.numeric(dat[,"type"] %in% "B")
  strikes <- as.numeric(dat[,"type"] %in% "S")
  pre.idx <- paste(dat[,"gameday_link"], dat[,"num"])
  idx <- factor(pre.idx, levels=unique(pre.idx))
  cum.balls <- unlist(tapply(balls, INDEX=idx, function(x){ n <- length(x); pmin(cumsum(c(0, x[-n])), 3) }))
  cum.strikes <- unlist(tapply(strikes, INDEX=idx, function(x) { n <- length(x); pmin(cumsum(c(0, x[-n])), 2) }))
  count <- paste(cum.balls, cum.strikes, sep = "-")
  return(cbind(dat, count))
}

# Add columns with relevant pitch count to the 'pitch' table.
# @param dat 'pitch' matrix/df
# @return returns the original matrix/df with the proper pitch count column appended.
appendDate <- function(dat) {
  if (!"gameday_link" %in% colnames(dat)){
    warning("'date' column couldn't be created")
    return(dat)
  }
  return(cbind(dat, date = substr(dat[,"gameday_link"], 5, 14)))
}

enhanced.scrape <- function (start, end, game.ids, suffix = "inning/inning_all.xml", 
                             connect, ...) 
{
  if (!missing(connect)) {
    if (!requireNamespace("DBI")) 
      warning("You will need the DBI package to write tables to a database.")
    fieldz <- plyr::try_default(DBI::dbListFields(connect, 
                                                  "atbat"), NULL, quiet = TRUE)
    if (!"date" %in% fieldz && !is.null(fieldz)) {
      msg <- "An 'atbat' table without the 'date' column was detected\n"
      if (!requireNamespace("dplyr") || packageVersion("dplyr") < 
          0.2) {
        message(msg, "To automatically append 'date', please install/update the dplyr and DBI packages \n", 
                "More details are discussed here -- \n", "http://baseballwithr.wordpress.com/2014/04/13/modifying-and-querying-a-pitchfx-database-with-dplyr/")
      }
      else {
        message(msg, "A 'date' column will now be appended. Please be patient.")
        new.col <- if ("SQLiteConnection" %in% class(connect)) {
          if ("gameday_link" %in% fieldz) 
            "SUBSTR(gameday_link, 15, -10)"
          else "SUBSTR(url, 80, -10)"
        }
        else {
          if ("gameday_link" %in% fieldz) 
            "SUBSTR(gameday_link, 5, 10)"
          else "SUBSTR(url, 70, 10)"
        }
        res <- DBI::dbSendQuery(connect, paste("CREATE TABLE atbat_temp AS SELECT *,", 
                                               new.col, "AS date FROM atbat"))
        DBI::dbRemoveTable(connect, name = "atbat")
        DBI::dbSendQuery(connect, "ALTER TABLE atbat_temp RENAME TO atbat")
      }
    }
  }
  message("If file names don't print right away, please be patient.")
  valid.suffix <- c("inning/inning_all.xml", "inning/inning_hit.xml", 
                    "miniscoreboard.xml", "players.xml")
  if (!all(suffix %in% valid.suffix)) {
    warning("Currently supported file suffix are: 'inning/inning_all.xml', 'inning/inning_hit.xml', 'miniscoreboard.xml', and 'players.xml'")
    Sys.sleep(5)
  }
  if (missing(game.ids)) {
    gameDir <- makeUrls(start = start, end = end)
  }
  else {
    if (!all(grepl("gid_", game.ids))) 
      warning("Any Game IDs supplied to the gids option should be of the form gid_YYYY_MM_DD_xxxmlb_zzzmlb_1")
    gameDir <- makeUrls(gids = game.ids)
  }
  fields = NULL
  env2 <- environment()
  data(fields, package = "pitchRx", envir = env2)
  if (any(grepl("miniscoreboard.xml", suffix))) {
    dayDir <- unique(gsub("/gid_.*", "", gameDir))
    scoreboards <- paste0(dayDir, "/miniscoreboard.xml")
    obs <- XML2Obs(scoreboards, as.equiv = TRUE, url.map = FALSE, 
                   ...)
    illegal <- paste0("games//game//", c("review", "home_probable_pitcher", 
                                         "away_probable_pitcher"))
    obs <- obs[!names(obs) %in% illegal]
    nms <- names(obs)
    nms <- gsub("^games//game//game_media//media$", "media", 
                nms)
    nms <- gsub("^games//game$", "game", nms)
    obs <- setNames(obs, nms)
    game.idx <- grep("^games$", nms)
    if (length(game.idx) > 0) 
      obs <- obs[-game.idx]
    tables <- collapse_obs2(obs)
    for (i in names(tables)) tables[[i]] <- format.table(tables[[i]], 
                                                         name = i)
    if (!missing(connect)) {
      for (i in names(tables)) export(connect, name = i, 
                                      value = tables[[i]], template = fields[[i]])
      rm(obs)
      rm(tables)
      message("Collecting garbage")
      gc()
    }
  }
  if (any(grepl("players.xml", suffix))) {
    player.files <- paste0(gameDir, "/players.xml")
    obs <- XML2Obs(player.files, as.equiv = TRUE, url.map = FALSE, 
                   ...)
    obs <- add_key(obs, parent = "game//team", recycle = "id", 
                   key.name = "name_abbrev", quiet = TRUE)
    obs <- add_key(obs, parent = "game//team", recycle = "type", 
                   quiet = TRUE)
    obs <- add_key(obs, parent = "game//team", recycle = "name", 
                   quiet = TRUE)
    nms <- names(obs)
    nms <- gsub("^game//team//player$", "player", nms)
    nms <- gsub("^game//team//coach$", "coach", nms)
    nms <- gsub("^game//umpires//umpire$", "umpire", nms)
    obs <- setNames(obs, nms)
    game.idx <- grep("game", nms)
    if (length(game.idx) > 0) 
      obs <- obs[-game.idx]
    if (exists("tables")) {
      tables <- c(tables, collapse_obs2(obs))
    }
    else {
      tables <- collapse_obs2(obs)
    }
    for (i in names(tables)) tables[[i]] <- format.table(tables[[i]], 
                                                         name = i)
    if (!missing(connect)) {
      for (i in names(tables)) export(connect, name = i, 
                                      value = tables[[i]], template = fields[[i]])
      rm(obs)
      rm(tables)
      message("Collecting garbage")
      gc()
    }
  }
  if (any(grepl("inning/inning_hit.xml", suffix))) {
    inning.files <- paste0(gameDir, "/inning/inning_hit.xml")
    obs <- XML2Obs(inning.files, as.equiv = TRUE, url.map = FALSE, 
                   ...)
    if (exists("tables")) {
      tables <- c(tables, collapse_obs2(obs))
    }
    else {
      tables <- collapse_obs2(obs)
    }
    names(tables) <- sub("^hitchart//hip$", "hip", names(tables))
    for (i in names(tables)) tables[[i]] <- format.table(tables[[i]], 
                                                         name = i)
    if (!missing(connect)) {
      for (i in names(tables)) export(connect, name = i, 
                                      value = tables[[i]], template = fields[[i]])
      rm(obs)
      rm(tables)
      message("Collecting garbage")
      gc()
    }
  }
  if (any(grepl("inning/inning_all.xml", suffix))) {
    inning.files <- paste0(gameDir, "/inning/inning_all.xml")
    n.files <- length(inning.files)
    cap <- min(200, n.files)
    if (n.files > cap && missing(connect)) {
      warning("play-by-play data for just the first 200 games will be returned (even though you've asked for", 
              n.files, ")", "If you want/need more, please consider using the 'connect' argument.")
    }
    n.loops <- ceiling(n.files/cap)
    for (i in seq_len(n.loops)) {
      inning.filez <- inning.files[seq(1, cap) + (i - 1) * 
                                     cap]
      inning.filez <- inning.filez[!is.na(inning.filez)]
      obs <- XML2Obs(inning.filez, as.equiv = TRUE, url.map = FALSE, 
                     ...)
      obs <- re_name(obs, equiv = c("game//inning//top//atbat//pitch", 
                                    "game//inning//bottom//atbat//pitch"), diff.name = "inning_side", 
                     quiet = TRUE)
      obs <- re_name(obs, equiv = c("game//inning//top//atbat//runner", 
                                    "game//inning//bottom//atbat//runner"), diff.name = "inning_side", 
                     quiet = TRUE)
      obs <- re_name(obs, equiv = c("game//inning//top//atbat//po", 
                                    "game//inning//bottom//atbat//po"), diff.name = "inning_side", 
                     quiet = TRUE)
      obs <- re_name(obs, equiv = c("game//inning//top//atbat", 
                                    "game//inning//bottom//atbat"), diff.name = "inning_side", 
                     quiet = TRUE)
      obs <- re_name(obs, equiv = c("game//inning//top//action", 
                                    "game//inning//bottom//action"), diff.name = "inning_side", 
                     quiet = TRUE)
      obs <- add_key(obs, parent = "game//inning", recycle = "num", 
                     key.name = "inning", quiet = TRUE)
      obs <- add_key(obs, parent = "game//inning", recycle = "next", 
                     key.name = "next_", quiet = TRUE)
      names(obs) <- sub("^game//inning//action$", "game//inning//atbat//action", 
                        names(obs))
      obs <- add_key(obs, parent = "game//inning//atbat", 
                     recycle = "num", quiet = TRUE)
      obs <- add_key(obs, parent = "game//inning//atbat", recycle = "pitcher", quiet = T)
      obs <- add_key(obs, parent = "game//inning//atbat", recycle = "batter", quiet = T)
      obs <- add_key(obs, parent = "game//inning//atbat", recycle = "stand", quiet = T)
      obs <- add_key(obs, parent = "game//inning//atbat", recycle = "b_height", quiet = T)
      obs <- add_key(obs, parent = "game//inning//atbat", recycle = "p_throws", quiet = T)
      nms <- names(obs)
      rm.idx <- c(grep("^game$", nms), grep("^game//inning$", 
                                            nms))
      if (length(rm.idx) > 0) 
        obs <- obs[-rm.idx]
      if (exists("tables")) {
        tables <- c(tables, collapse_obs2(obs))
      }
      else {
        tables <- collapse_obs2(obs)
      }
      rm(obs)
      gc()
      tab.nms <- names(tables)
      tab.nms <- sub("^game//inning//atbat$", "atbat", 
                     tab.nms)
      tab.nms <- sub("^game//inning//atbat//action$", "action", 
                     tab.nms)
      tab.nms <- sub("^game//inning//atbat//po$", "po", 
                     tab.nms)
      tab.nms <- sub("^game//inning//atbat//runner$", "runner", 
                     tab.nms)
      tab.nms <- sub("^game//inning//atbat//pitch$", "pitch", 
                     tab.nms)
      tables <- setNames(tables, tab.nms)
      scrape.env <- environment()
      data(players, package = "pitchRx", envir = scrape.env)
      players$id <- as.character(players$id)
      colnames(tables[["atbat"]]) <- sub("^batter$", "id", 
                                         colnames(tables[["atbat"]]))
      tables[["atbat"]] <- merged(x = tables[["atbat"]], 
                                  y = players, by = "id", all.x = TRUE)
      colnames(tables[["atbat"]]) <- sub("^id$", "batter", 
                                         colnames(tables[["atbat"]]))
      colnames(tables[["atbat"]]) <- sub("^full_name$", 
                                         "batter_name", colnames(tables[["atbat"]]))
      colnames(tables[["atbat"]]) <- sub("^pitcher$", "id", 
                                         colnames(tables[["atbat"]]))
      tables[["atbat"]] <- merged(x = tables[["atbat"]], 
                                  y = players, by = "id", all.x = TRUE)
      colnames(tables[["atbat"]]) <- sub("^id$", "pitcher", 
                                         colnames(tables[["atbat"]]))
      colnames(tables[["atbat"]]) <- sub("^full_name$", 
                                         "pitcher_name", colnames(tables[["atbat"]]))
      colnames(tables[["atbat"]]) <- sub("^des", "atbat_des", 
                                         colnames(tables[["atbat"]]))
      for (i in names(tables)) tables[[i]] <- format.table(tables[[i]], 
                                                           name = i)
      tables[["pitch"]] <- appendPitchCount(tables[["pitch"]])
      tables[["atbat"]] <- appendDate(tables[["atbat"]])
      if (!missing(connect)) {
        for (i in names(tables)) export(connect, name = i, 
                                        value = tables[[i]], template = fields[[i]])
        rm(tables)
        message("Collecting garbage")
        gc()
      }
    }
  }
  if (exists("tables")) {
    return(tables)
  }
  else {
    return(NULL)
  }
}

##############
### SCRAPE ### 
##############
pack <- c("dplyr", "pitchRx", "DBI", "XML2R", "readr", "MASS", "ggplot2", "spatstat", "lattice", "car", "dummies", "mnormt")
suppressMessages(suppressWarnings(lapply(pack, require, character.only = TRUE, quietly = T)))
setwd("~/GitHub/Summer2019/Kershaw Model/hierarchical kershaw model")

# db <- src_sqlite("baseball_dat_2014-06-19_2018-12-31", create = F)
# enhanced.scrape(start = "2018-08-01", end = "2017-12-31", connect = db$con)
# db2 <- src_sqlite("player_dat_2008-08-01_2017-12-31", create = F)
# enhanced.scrape(start = "2008-08-01", end = "2017-12-31", suffix = "players.xml", connect = db2$con)

db <- src_sqlite("baseball_dat_2008-08-01_2014-06-18", create = F)
db2 <- src_sqlite("baseball_dat_2014-06-19_2017-12-31", create = F)
db3 <- src_sqlite("player_dat_2008-08-01_2017-12-31", create = F)

pitch <- tbl(db, "pitch")
kershaw.pitch <- collect(filter(pitch, pitcher == "477132"))
pitch2 <- tbl(db2, "pitch")
kershaw.pitch2 <- collect(filter(pitch2, pitcher == "477132"))
kershaw <- rbind(kershaw.pitch, kershaw.pitch2)

player <- tbl(db3, "player")
player.df <- collect(player, n = Inf)

kershaw$batter <- as.numeric(kershaw$batter)
player.df$batter <- as.numeric(player.df$id)
player.df <- dplyr::select(player.df, -id, -num, -url, -type)
kershaw <- inner_join(kershaw, player.df, by = c("batter", "gameday_link"))
kershaw$gameday_link <- factor(kershaw$gameday_link)

save(kershaw, file = 'kershaw.Rdata')