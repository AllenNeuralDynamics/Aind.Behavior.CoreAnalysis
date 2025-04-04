import harp

a = harp.create_reader(r"C:\Users\bruno.cruz\Downloads\789903_2025-04-02T182737Z\behavior\Behavior.harp")
print(a.WhoAmI.read())
