import sys
import utils
import time

start__ = time.time()


input_file = sys.argv[1]
k = int(sys.argv[2])
output_file = sys.argv[3]

data = utils.get_points(input_file)

ds, retain_set, ds_track = utils.bfr_init(data, k)

retain_set, cs, cs_track = utils.bfr_form_cs(retain_set, {})

f = open(output_file, "w+")
f.write("The intermediate results:\n")

f.write("Round 1: " + str(utils.get_number_of_points_in_ds(ds)) + "," + str(len(cs)) + "," + str(utils.get_number_of_points_in_ds(cs)) + "," + str(len(retain_set)))
f.write("\n")


start = 0.2
end = 0.4

for i in range(4):

	ds, cs, retain_set, ds_track, cs_track = utils.bfr_main(ds, cs, retain_set, data[int(len(data)*start):int(len(data)*end)], ds_track, cs_track)

	f.write("Round " + str(i+2) + ": " + str(utils.get_number_of_points_in_ds(ds)) + "," + str(len(cs)) + "," + str(utils.get_number_of_points_in_ds(cs)) + "," + str(len(retain_set)))
	f.write("\n")
	start = end
	end += 0.2


ds, ds_track = utils.merge_ds_cs(ds, cs, ds_track, cs_track)

f.write("Round 6" + ": " + str(utils.get_number_of_points_in_ds(ds)) + ",0,0," + str(len(retain_set)))
f.write("\n")

f.write("The clustering results:")
f.write("\n")

key_value = []
for key in ds_track:
	for point in ds_track[key]:
		key_value.append((point, key))

key_value = sorted(key_value, key=lambda x: x[0])
for item in key_value:
	f.write(str(item[0]) + "," + str(item[1]))
	f.write("\n")
for point in retain_set:
	f.write(str(point[0]) + "," + str(-1))
	f.write("\n")

f.close()

end__ = time.time()
