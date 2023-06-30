import os
import math
import soundfile as sf

directory = "*****"
dura = 0
costtotal = 0
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".wav"):
        print("=====================================")
        print("Leyendo {}".format(filename))
        f = sf.SoundFile(directory + "\\" + filename)
        #print('samples = {}'.format(len(f)))
        #print('sample rate = {}'.format(f.samplerate))
        secs = math.ceil(len(f) / f.samplerate)
        dura += secs
        print('segundos = {}'.format(secs))
        coste = max(secs,15.) * 0.0004
        costtotal += coste
        print('coste = {} USD'.format(coste))

print('Duracion Total = {} segundos'.format(dura))
print('Coste Total = {} USD + IVA'.format(costtotal))
