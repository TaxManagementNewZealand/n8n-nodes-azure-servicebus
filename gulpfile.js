const gulp = require('gulp');

// Copy SVG icons from source to dist
gulp.task('build:icons', function() {
  return gulp.src('nodes/**/*.svg')
    .pipe(gulp.dest('dist/nodes'));
});
