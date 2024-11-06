#!/bin/env python3

import os
import sys

samps = sys.argv[1]
# samps = os.getcwd().split("/")[-1]

NT_positions = []
asdflas = [
    "100",
    "178",
    "241",
    "335",
    "378",
    "632",
    "647",
    "686",
    "687",
    "689",
    "691",
    "692",
    "2269",
    "2470",
    "2832",
    "3037",
    "3745",
    "3967",
    "4181",
    "4184",
    "5386",
    "5648",
    "5779",
    "6080",
    "6120",
    "6135",
    "6441",
    "6513",
    "6514",
    "6515",
    "7977",
    "8140",
    "8393",
    "8739",
    "9204",
    "9344",
    "9707",
    "9711",
    "9751",
    "9992",
    "9994",
    "10029",
    "10030",
    "10039",
    "11081",
    "11279",
    "11283",
    "11284",
    "11285",
    "11286",
    "11287",
    "11288",
    "11289",
    "11290",
    "11291",
    "11537",
    "11947",
    "11950",
    "11956",
    "11960",
    "11963",
    "13195",
    "13647",
    "13729",
    "13730",
    "13731",
    "13732",
    "13827",
    "13828",
    "13829",
    "13830",
    "13831",
    "13832",
    "13833",
    "13834",
    "13835",
    "13836",
    "13837",
    "13838",
    "13839",
    "13840",
    "13928",
    "13929",
    "13930",
    "14201",
    "14320",
    "14408",
    "14409",
    "14410",
    "14411",
    "14412",
    "14413",
    "14414",
    "14415",
    "14416",
    "14419",
    "14420",
    "14543",
    "14541",
    "14542",
    "14543",
    "14544",
    "14545",
    "15414",
    "15424",
    "15240",
    "15474",
    "15490",
    "15735",
    "15907",
    "15956",
    "17051",
    "17054",
    "18105",
    "18163",
    "18421",
    "19080",
    "19684",
    "19955",
    "20055",
    "20393",
    "20407",
    "21255",
    "21595",
    "21603",
    "21762",
    "21765",
    "21766",
    "21767",
    "21768",
    "21769",
    "21770",
    "21846",
    "21789",
    "21846",
    "21987",
    "21988",
    "21989",
    "21990",
    "21991",
    "21992",
    "21993",
    "21994",
    "21995",
    "22194",
    "22195",
    "22196",
    "22012",
    "22069",
    "22070",
    "22079",
    "22081",
    "22083",
    "22104",
    "22194",
    "22195",
    "22196",
    "22449",
    "22566",
    "22578",
    "22661",
    "22673",
    "22682",
    "22685",
    "22686",
    "22690",
    "22691",
    "22692",
    "22713",
    "22813",
    "22882",
    "22884",
    "22896",
    "22898",
    "22907",
    "22942",
    "22992",
    "22995",
    "23010",
    "23012",
    "23013",
    "23018",
    "23019",
    "23039",
    "23040",
    "23048",
    "23055",
    "23063",
    "23075",
    "23140",
    "23141",
    "23142",
    "23179",
    "23180",
    "23184",
    "23188",
    "23200",
    "23202",
    "23212",
    "23219",
    "23237",
    "23403",
    "23481",
    "23483",
    "23525",
    "23599",
    "23604",
    "23854",
    "23948",
    "24130",
    "24153",
    "24380",
    "24424",
    "24469",
    "24480",
    "24503",
    "25000",
    "25020",
    "25584",
    "25708",
    "26111",
    "26182",
    "26183",
    "26270",
    "26527",
    "26530",
    "26538",
    "26577",
    "26590",
    "26709",
    "27259",
    "27476",
    "27807",
    "28085",
    "28311",
    "28362",
    "28363",
    "28364",
    "28365",
    "28366",
    "28367",
    "28368",
    "28369",
    "28370",
    "28472",
    "28881",
    "28882",
    "28883",
    "29249",
    "29332",
    "29510",
]
NTcall_variants = [] # ['C241CandC3037TandC14408CandA23403G'] # [] # ['C24044T'] # ['C1059TandC3543TandC7635GandA18982GandA23013CandA23056CandC23117AandT23119CandA23403GandG23576TandC23604AandC26060T'] # ['C241TandC14408CandA17496CandA22893CandA23013CandA23056CandA23403GandA25020CandA27330CandA28271TandA4178CandA5648CandA6328GandA9204GandC1059TandC11916TandC1616AandC23029TandC23039AandC23054TandC23117AandC23277AandC24044TandC24418TandC25936GandC27920TandC28887TandC3037TandC3267TandC4113TandC4230TandC5178AandC9711TandG11670AandG17196AandG22340AandG22599AandG25019AandG25116AandG25563TandG25947CandG29540AandG3849TandG9479TandT18660CandT22907CandT23031AandT23406CandT25570AandT27322CandT27384CandT27907GandT27929AandT5507GandT8296CandT9982C']
NT_multi_variants = [] # ['C241CandC3037TandC14408CandA23403GandG25563T']
#L452R and T478K, but not S477N.
AA_variants = ["E484del"] # "Q498Y", "Q498H", ["A1250C(K417T) A1331C(K444T) T1334C(V445A) T1345C(Y449H) T1355G(L452R) T1380G(N460K) A1451C(E484A) TTT1456-1458CCT(F486P) T1480G(S494A) CAA1492-1494TAT(Q498Y) C1495T(P499S) A1498T(T500S) A1502C(N501T) T1513C(Y505H)"] #['CTACAAGTT14408-14416del'] # ['Reference'] # ['L452Q'] # ['GCTA13729-13732Del'] # ["K417"and"N440K"and"G446S"and"L452R"and"S477N"and"N460K"and"K444"and"Q493"and"E484"and"Q498"and"1450-1452Del"and"N501Y"and"Y505H"] # [] # ['L828F'] #    ['K444E'and'D574E'] #
multi_var = [] # ['N440EandL441RandK444ΔandK444SandK444TandV445ΔandV445AandV445NandG446ΔandG446DandY449HandY449RandY449SandL452KandY453FandL455WandF456LandN460KandT470NandT478RandV483AandV483ΔandE484ΔandE484PandF486AandF486PandF486VandF490HandF490PandF490YandQ493KandnotQ493RandQ493YandQ498HandQ498KandQ498YandQ498KandnotQ489RandP499HandP499SandN501SandN501TandY505NandY508HandH519NandT572IandT572N'] #  ["K417TandK444TandV445AandY449HandL452RandN460KandE484AandF486PandS494AandQ498YandP499SandT500SandN501TandY505H"] # ['K444TandY449RandN460KandE484AandF486PandQ493KandQ493RandQ498HandQ498YandN501SandN501TandY505H'] # ['440Eand441Rand444Sand445Nand446Dand449Hand449Rand449Sand452Qand452Kand453Fand455Wand456Land484Qand484Pand484Vand486Aand486Pand486Vand490Hand490Pand490Yand494Pand498Hand498Yand498Kand505Nand572I'] # ['K444TandV445AandY449andL452QandN460KandE484PandF486PandF490YandQ493KandQ498HandQ498YandN501SandN501T']
Omis = {"1" : {"NTD" : ["A67V", "T95I", "425-433Del", "632-634Del", "215EPE"], "RBD" : ["1251T(K417N)", "1320G(N440K)", "1336A(G446S)", "1430A(S477N)", "1433A(T478K)", "1451C(E484A)", "1478G(Q493R)", "1486A(G496S)", "1493G(Q498R)", "1501T(N501Y)", "1513C(Y505H)", "1640A(T547K)"]},
        "2" : {"NTD" : ["not A67V", "not T95I", "G142D", "not 425-433Del", "not 632-634Del", "V213G"], "RBD" : ["1251T(K417N)", "1320G(N440K)", "not G446", "1430A(S477N)", "1433A(T478K)", "1451C(E484A)", "1478G(Q493R)", "not G496", "1493G(Q498R)", "1501T(N501Y)", "1513C(Y505H)", "not T547"]},
        "3" : {"NTD" : ["A67V", "T95I", "425-433Del", "632-634Del", "not 215EPE"], "RBD" : ["1251T(K417N)", "1320G(N440K)", "1336A(G446S)", "1430A(S477N)", "1433A(T478K)", "1451C(E484A)", "1478G(Q493R)", "not G496", "1493G(Q498R)", "1501T(N501Y)", "1513C(Y505H)", "not T547"]}}
Search_Omis = 0
search_ref = 0
Omi_matches = {}
NT_outfiles = {}
NT_mult_outfiles = {}
AA_outfiles = {}
multivar_fhs = {}
nt_call_dict = {}
NT_SRAs = {}
NT_Sample_dict = {}
NT_mult_Sample_dict = {}
Ref_Sample_dict = {}
POS_Sample_dict = {}
ref_NT_dict = {}

if NTcall_variants:
    i = 2
    for variant in NTcall_variants:
        NT_Sample_dict[variant] = {}
        Ref_Sample_dict[variant] = {}
        NT_outfiles[variant] = open(samps+"_NT_"+str(i)+".tsv","w")
        i += 1
if NT_multi_variants:
    for variant in NT_multi_variants:
        NT_mult_Sample_dict[variant] = {}
        NT_mult_outfiles[variant] = open(samps+"_NTmult_"+variant+".tsv","w")

if AA_variants:
    for variant in AA_variants:
        AA_outfiles[variant] = open(samps+"_AA_"+variant+".tsv","w")
if multi_var:
    i = 1
    for variant in multi_var:
        multivar_fhs[variant] = open(samps+"_multi"+str(i)+".tsv","w")
        multivar_fhs[variant].write(variant)
        multivar_fhs[variant].write("\n")
        i += 1
if search_ref == 1:
    ref_fh = open(samps+"_refs.tsv", "w")
files_read = []

testcounter = 0
for subdir, dirs, files in os.walk(os.getcwd()):
    for file in files:
        if file not in files_read and "Assemblies" not in subdir:
            files_read.append(file)
            if (file.endswith("_unique_seqs.tsv")) and (AA_variants or multi_var or NT_multi_variants) and "_AA_" not in file: # and not 'wgs' in file:  or file.endswith('_reads.tsv') or file.endswith('_covars.tsv')
                in_file = open(os.path.join(subdir, file))
                NT_multi_matches = {}
                AA_matches = {}
                mv_matches = {}
                counts = 0
                ft = 0
                if file.endswith("_reads.tsv"):
                    ft = 1

                for line in in_file:
                    splitline = line.split("\t")
                    try:
                        splitline[1]
                    except:
                        pass
                    else:
                        if splitline[1] != "Count":
                            if NT_multi_variants:
                                for variant in NT_multi_variants:
                                    mismatch = 0
                                    first = variant.split("and")[0][1:-1]
                                    try:
                                        if int(splitline[ft].split(" ")[0]) > int(first):
                                            continue
                                    except:
                                        pass

                                    for PM in variant.split("and"):
                                        if "not" in PM:
                                            curPM = PM.strip("not")
                                            if curPM[0] == curPM[-1]:
                                                if PM[:-1] in splitline[ft]:
                                                    # for nt in ['A', 'T', 'C', 'G', 'N', '-']:
                                                    # if PM[:-1]+nt in splitline[0]:
                                                    mismatch += 1
                                            elif curPM in splitline[ft]:
                                                mismatch += 1
                                        elif PM[0] == PM[-1]:
                                            if PM[:-1] in splitline[ft]:
                                                # for nt in ['A', 'T', 'C', 'G', 'N', '-']:
                                                # if PM[:-1]+nt in splitline[0]:
                                                mismatch += 1
                                        elif PM not in splitline[ft]:
                                            mismatch += 1
                                    if mismatch == 0:
                                        try:
                                            NT_multi_matches[variant].append(line) # str(mismatch) + "\t" + line)
                                            # counts += int(splitline[1])
                                        except:
                                            NT_multi_matches[variant] = [line]
                            if ft == 1 or (int(splitline[1]) >= 4):
                                if multi_var:
                                    for variant in multi_var:
                                        mismatch = 0
                                        match = 0
                                        for PM in variant.split("and"):
                                            if "not" in PM:
                                                if PM.strip("not") in splitline[ft]:
                                                    mismatch += 1
                                            elif PM in splitline[ft]:
                                                match += 1

                                        if ((match - mismatch) > 1) and len(splitline[ft].split(" ")) < 25 and splitline[ft].count("insert") < 3 and splitline[ft].count("Del") < 4:
                                            try:
                                                mv_matches[variant].append(line)
                                            except:
                                                mv_matches[variant] = [line]
                            if AA_variants:
                                if (int(splitline[1]) > 0 or ft == 1):
                                    for variant in AA_variants:
                                        if variant in splitline[ft]:
                                            try:
                                                AA_matches[variant].append(line)
                                            except:
                                                AA_matches[variant] = [line]

                if NT_multi_matches:
                    for variant in NT_multi_matches:
                        NT_mult_outfiles[variant].write(file[:-4]+"\t")
                        NT_mult_outfiles[variant].write("\n")
                        for line in NT_multi_matches[variant]:
                            NT_mult_outfiles[variant].write(line)
                if mv_matches:
                    for variant in mv_matches:
                        multivar_fhs[variant].write(subdir+"/"+file[:-4]+"\t")
                        multivar_fhs[variant].write("\n")
                        for line in mv_matches[variant]:
                            multivar_fhs[variant].write(line)
                if AA_matches:
                    for variant in AA_matches:
                        if AA_matches[variant]:
                            AA_outfiles[variant].write(subdir+"/"+file+"\n")
                            for line in AA_matches[variant]:
                                AA_outfiles[variant].write(line)

                in_file.close()

            if file.endswith("_nt_calls.tsv") and (NTcall_variants or NT_positions):
                in_file = open(os.path.join(subdir, file))
                match_lines = {}
                match_dict = {}
                ref_dict = {}
                for line in in_file:
                    splitline = line.split("\t")
                    try:
                        splitline[1]
                    except:
                        pass
                    else:
                        if splitline[0] == "Position":
                            PosLine = line
                            for i in range(len(splitline)):
                                nt_call_dict[splitline[i]] = i
                        if NT_positions:
                            if splitline[0] in NT_positions:
                                try:
                                    POS_Sample_dict[file.split("_nt_calls")[0]][splitline[0]] = (int(splitline[int(nt_call_dict[splitline[1]])]) / int(splitline[int(nt_call_dict["Total"])]))
                                except:
                                    POS_Sample_dict[file.split("_nt_calls")[0]] = { splitline[0] : (int(splitline[int(nt_call_dict[splitline[1]])]) / int(splitline[int(nt_call_dict["Total"])]))}
                                try:
                                    ref_NT_dict[splitline[0]]
                                except:
                                    ref_NT_dict[splitline[0]] = splitline[1]


                        if NTcall_variants:
                            for var in NTcall_variants:
                                try:
                                    match_lines[var]
                                except:
                                    match_lines[var] = []
                                # if 'and' in var:
                                varPMs = var.split("and")
                                for subvar in varPMs:
                                    if subvar[1:-1] == splitline[0]:
                                        if int(splitline[int(nt_call_dict["Total"])]) > 50 and (int(splitline[int(nt_call_dict[subvar[-1]])]) > (.2 * int(splitline[int(nt_call_dict["Total"])]))):
                                            match_lines[var].append(line)
                                            try:
                                                match_dict[var][subvar] = (int(splitline[int(nt_call_dict[subvar[-1]])]) / int(splitline[int(nt_call_dict["Total"])]))
                                            except:
                                                match_dict[var] = { subvar : (int(splitline[int(nt_call_dict[subvar[-1]])]) / int(splitline[int(nt_call_dict["Total"])])) }
                                            try:
                                                ref_dict[var][subvar] = (int(splitline[int(nt_call_dict[subvar[0]])]) / int(splitline[int(nt_call_dict["Total"])]))
                                            except:
                                                ref_dict[var] = { subvar : (int(splitline[int(nt_call_dict[subvar[0]])]) / int(splitline[int(nt_call_dict["Total"])])) }
                            # elif var[1:-1] == splitline[0]:
                            # if int(splitline[int(nt_call_dict['Total'])]): # > 50 and (int(splitline[int(nt_call_dict[subvar[-1]])]) > (.01 * int(splitline[int(nt_call_dict['Total'])]))):
                            # match_lines[var].append(line)

                if match_lines:

                    for variant in match_lines:
                        if len(match_lines[variant]) == len(variant.split("and")):
                            try:
                                NT_Sample_dict[variant][file.split(".")[0]] = match_dict[variant]
                                Ref_Sample_dict[variant][file.split(".")[0]] = ref_dict[variant]
                                NT_outfiles[variant].write(file.split(".")[0]+"\n")
                                NT_outfiles[variant].write("\t".join(PosLine.split("\t")[:int(nt_call_dict["Total"])+1])+"\tVarient\tAbundance\n")
                                for line in match_lines[variant]:
                                    splitline= line.split("\t")
                                    NT_outfiles[variant].write("\t".join(splitline[:int(nt_call_dict["Total"])+1]))
                                    for subvar in var.split("and"):
                                        if subvar[1:-1] == splitline[0]:
                                            NT_outfiles[variant].write(f"\t{subvar}\t{match_dict[variant][subvar]}")
                                    NT_outfiles[variant].write("\n")
                                try:
                                    NT_SRAs[variant].append(file.split(".")[0])
                                except:
                                    NT_SRAs[variant] = [(file.split(".")[0])]
                            except:
                                pass


                in_file.close()

            if file.endswith("_chim_rm.tsv") or file.endswith("_covar_deconv.tsv"):
                if Search_Omis == 1 or search_ref == 1:
                    samp_name = ""
                    if file.endswith("_covar_deconv.tsv"):
                        samp_name = file.strip("_covar_deconv.tsv")
                    else:
                        samp_name = "_".join(file.split("_")[:-3])
                    ref_lines = []
                    amp = ""
                    if "NTD" in file:
                        amp = "NTD"
                    elif "S1S2" in file:
                        amp = "S1S2"
                    elif "RBD" in file:
                        amp = "RBD"
                    if amp:
                        in_file = open(os.path.join(subdir, file))
                        counts = {"1" : [0 ,0 ,0],
                                  "2" : [0 ,0 ,0],
                                  "3" : [0 ,0 ,0]} # counts['2'][1] == abundance of seq that had 1 mismatches to BA.2
                        matched = 0
                        for line in in_file:
                            splitline = line.strip("\n\r").split("\t")
                            try:
                                splitline[2]
                            except:
                                pass
                            else:
                                if splitline[1] != "Count":
                                    split_seq = splitline[0].split(" ")
                                    if Search_Omis == 1:
                                        if amp != "S1S2":
                                            for sub in Omis:
                                                mismatches = 0
                                                for PM in Omis[sub][amp]:
                                                    if "not" in PM:
                                                        if PM.split(" ")[1] in splitline[0]:
                                                            mismatches += 1
                                                    elif PM not in splitline[0]:
                                                        mismatches += 1
                                                if mismatches < 3:
                                                    matched = 1
                                                    counts[sub][mismatches] += float(splitline[2])
                                                    # print(splitline[0] + "\t" + sub + "\t" + str(mismatches) + "\t" + splitline[2])
                                                    # print(counts[sub][mismatches])
                                    if search_ref == 1:
                                        # if float(splitline[2]) >= .01: # and not '/NY/' in subdir:
                                        if amp == "S1S2" and splitline[0] == "1841G(D614G)" or splitline[0] == "Reference":
                                            ref_lines.append(line)
                    if ref_lines:
                        ref_fh.write(os.path.join(subdir, file)+"\n")
                        for line in ref_lines:
                            ref_fh.write(line)
                        ref_fh.write("\n")



                        # print(counts)
                        if matched == 1:
                            try:
                                Omi_matches[samp_name]
                            except:
                                Omi_matches[samp_name] = counts
                            else:
                                for subvar in Omi_matches[samp_name]:
                                    for i in range(3):
                                        Omi_matches[samp_name][subvar][i] += counts[subvar][i]


                    in_file.close()

if search_ref == 1:
    ref_fh.close()

if multi_var:
    for variant in multivar_fhs:
        multivar_fhs[variant].close()
if NT_mult_outfiles:
    for variant in NT_mult_outfiles:
        NT_mult_outfiles[variant].close()
if NTcall_variants:
    for variant in NTcall_variants:
        NT_outfiles[variant].close()
if AA_variants:
    for variant in AA_variants:
        AA_outfiles[variant].close()
if NT_SRAs:
    for variant in NT_SRAs:
        print(variant + " " + str(len(NT_SRAs[variant])))
        # for SRA in NT_SRAs[variant]:
        # print(SRA)

if POS_Sample_dict:
    fh_POS_table = open(samps + "_RefNTtable.tsv", "w")
    fh_POS_table.write("Position")
    for sample in POS_Sample_dict:
        fh_POS_table.write(f"\t{sample}")
    fh_POS_table.write("\n")
    for position in NT_positions:
        fh_POS_table.write(ref_NT_dict[position]+position)
        for sample in POS_Sample_dict:
            try:
                fh_POS_table.write(f"\t{POS_Sample_dict[sample][position]:04f}")
            except:
                fh_POS_table.write("\t")
        fh_POS_table.write("\n")


    fh_POS_table.close()

if NT_Sample_dict:
    i = 2
    for variant in NT_Sample_dict:
        fh_NT_table = open(samps + "_" + str(i) + "_NTtable.tsv", "w")
        i += 1
        fh_NT_table.write("mut")
        for sample in NT_Sample_dict[variant]:
            fh_NT_table.write(f"\t{sample}")
        fh_NT_table.write("\n")
        for subvar in variant.split("and"):
            fh_NT_table.write(subvar)
            for sample in NT_Sample_dict[variant]:
                try:
                    fh_NT_table.write(f"\t{NT_Sample_dict[variant][sample][subvar]:04f}")
                except:
                    fh_NT_table.write("\t")
            fh_NT_table.write("\n")


        fh_NT_table.close()

if Ref_Sample_dict:
    i = 2
    for variant in Ref_Sample_dict:
        fh_Ref_table = open(samps + "_" + str(i) + "_Reftable.tsv", "w")
        i += 1
        fh_Ref_table.write("mut")
        for sample in Ref_Sample_dict[variant]:
            fh_Ref_table.write(f"\t{sample}")
        fh_Ref_table.write("\n")
        for subvar in variant.split("and"):
            fh_Ref_table.write(subvar[:-1]+subvar[0])
            for sample in Ref_Sample_dict[variant]:
                try:
                    fh_Ref_table.write(f"\t{Ref_Sample_dict[variant][sample][subvar]:04f}")
                except:
                    fh_Ref_table.write("\t")
            fh_Ref_table.write("\n")


        fh_Ref_table.close()


if Omi_matches:
    Omi_out_fh = open(samps+"_Omis.tsv","w")
    sorted_samps = sorted(Omi_matches.keys())
    for subvar in Omis:
        for domain in Omis[subvar]:
            Omi_out_fh.write(f"BA.{subvar}\t{domain}\t{', '.join(Omis[subvar][domain])}")
            Omi_out_fh.write("\n")
    Omi_out_fh.write("\n")
    # Omi_out_fh.write(f"\t\t[Perfect match, 1 mismatch, 2 mismatch]")
    for key in sorted_samps:
        Omi_out_fh.write(key+ "\t\tPerfect match\t1 mismatch\t2 mismatch\n")
        for subvar in Omi_matches[key]:
            Omi_out_fh.write(f"\tBA.{subvar}")
            for val in Omi_matches[key][subvar]:
                Omi_out_fh.write(f"\t{val}")
            Omi_out_fh.write("\n")
        Omi_out_fh.write("\n")
    Omi_out_fh.close()
