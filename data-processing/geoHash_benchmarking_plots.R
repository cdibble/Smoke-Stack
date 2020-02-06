geoHash_benchmarking_plots.R
require(ggplot2)
require(reshape2)
require(dplyr)
require(viridis)
df = read.csv("/Users/Connor/Documents/Graduate School/Dibble_Research/Github_repos/ship-soot/data-processing/geoHash_BenchMark_Experiment.csv")
df <- df[!is.na(df$cell_size_km),]
plot.df <- df %>% select(cell_size_km, pings.count.., pings.Null.count.., pings.NotNull.count..)
names(plot.df) <- c("cell_size_km", "Total_pings", "Pings_outside_geoHash", "Pings_inside_geoHash")
plot.df <- melt(plot.df, id.vars = "cell_size_km")

exp.plot <- ggplot(plot.df) + geom_line(aes(x = cell_size_km, y = value, color = variable), size = 2) +
	geom_vline(aes(xintercept = 20), linetype = 3) + geom_vline(aes(xintercept = 40), linetype = 3) +
	xlab("geoHash Grid Cell Size (km)") + ylab("Ping Counts") + scale_color_viridis(discrete = TRUE) + theme_bw() +
	theme(text = element_text(size = 14), legend.position = c(0.85, 0.1), legend.background = element_rect(color = 'black', size = 0.1),
	legend.title = element_blank())

png(file = "/Users/Connor/Documents/Graduate School/Dibble_Research/Github_repos/ship-soot/data-processing/geoHash_benchmarking_figure.png", height = 7.5, width = 10, units = 'in', res = 400)
	print(exp.plot)
dev.off()

