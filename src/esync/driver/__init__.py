



def callback(function, progress, total):
    if function:
        function(progress, total)
        
        

def print_progress_callback(progress, total):
    print "progress is at %d from %d" % (progress, total)
