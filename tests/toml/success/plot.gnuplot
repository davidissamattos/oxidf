set datafile separator ";"
set autoscale fix
set key left

set title "Cars"
set xlabel "Distance"
set ylabel "Speed"
set style data linespoints
plot "./tests/toml/cars_semicolon.csv" using (column("speed")):(column("dist")) lw 2 title "Speed vs Dist"

pause -1