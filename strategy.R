#load external libs
source("https://raw.githubusercontent.com/RWLab/rwRtools/master/examples/colab/load_libraries.R")
pacman::p_load_current_gh("Robot-Wealth/rsims", dependencies = TRUE)
#load_libraries(load_rsims = TRUE, extra_libraries = c('quantmod', 'patchwork', 'roll'), extra_dependencies = c())

# Set chart options
options(repr.plot.width = 14, repr.plot.height=7)
theme_set(theme_bw())
theme_update(text = element_text(size = 20))
install.packages("languageserver")
