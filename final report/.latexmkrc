ensure_path( 'TEXINPUTS', './acmart-primary//' );

# Configure output directory  
$out_dir = 'out';

# Clean up empty subdirectories after build
END {
    system("find out -type d -empty -delete 2>/dev/null || true");
}
