
# =============================================================================
# 1. The dicts can be ignored, we use the pattern-name pair list for the regex
#    matching. (and they are not maintained)
# 2. If the pattern-name are in a list, then the order matters.
#    -> specific and then general
# =============================================================================

# TODO: Since some of the name require sequencial processing, we might need two list.
# some common mistakes for Homes in data: Gomes, Jomes, IIomes, I-Iomes
# The matching strings are in regex.
builders_list_1 = {
    r'''D(\.| )?( )?R(\.| )?( )?(-)?\bH[^ KZSHMN]{4,7}(M|N)?\b|
        D(\.| )?( )?R(\.| )?( )?(-)?[HD]O[^ Z]*ON''':'D.R. HORTON',
    r"LENNER|^LENAR|^LENNR|^LENNAT|^LNR LENNAR|LANNAR|LENNNAR|LENNAR": 'LENNAR',
    r"^PLUTE|^PAULTE|PULTE HM|^PULTE|PULTEGROUP": 'PULTEGROUP',  # centex?
    r"TOLL BR|TOLLBRO": 'TOLL BROTHERS',  # COLEMAN TOLL?
    r"^N V R|NVR": 'NVR',
    r'''^TAYLOR MORRSION|TAYOR MORRISON|TAYLOR MARRISON|TAYLOR MORISON|TAYLOR MONISON|TM HOMES|TAYLOR MORRISON|
        ^MORRISON HM|^MORISON HOMES|MORISSON HOMES''': 'TAYLOR MORRISON',
    r"^KB H(O)?M|^KB H0MES|^KAUFMAN & B|^KUAFMAN & B|^KAUFMAN B|KB-HOME": 'KB HOME',
    r'''^MERITAE|^MERITAG|^MERITA.E HOMES|^MERITAQ|^MERITGAE|^MERITGAGE HOMES|^MERITAHE HOMES|
        ^MERITFAGE|^MERITGE|^MERJTAGE HOMES|MERITAGE HOMES''': 'MERITAGE HOMES',
    r"RICHM.* AM|^RICHMONDAM|^RICHMOND MERICAN|^RICHO.* AM|RICHMOND AMERICAN": 'M.D.C. HOLDINGS',
    r"^CLAYTON HOM|^CLAYTON HM|CLAYTON PRO|^CLAYON PRO": 'CLAYTON PROPERTIES GROUP',
    r"M/I H(O)?M|^M/I . HOME|^M/I. HOME|^M/.HOME|^M/ .HOME|^M/. HOME|^M/HOME|^MI HM|^MI HOME": 'M/I HOMES',
    r'''^DREAM FINDER|^DFH CAPITOL|^DFH CLOVER|^DFH CORONA|^DFH GREYHAWK|^DFH JOHNS LANDING|^DFH LAND|
        ^DFH MANDARIN|^DFH WILDWOOD''': 'DREAM FINDERS HOMES', # https://www.sec.gov/Archives/edgar/data/1825088/000114036121010794/brhc10022392_ex21-1.htm
    r"^TRI POINT|^TRI POLNT|^TRI PONTE|TRI-POINT": 'TRI POINTE HOMES',
    r"CENTURY COM|CENTURYCOM": 'CENTURY COMMUNITIES',
    r"^ASHTON W|^HOMES ASHTON W|ASHTONW": 'ASHTON WOODS',
    r"^DAVID WEEK": 'DAVID WEEKLEY HOMES',  # DW HOMES seems to be a different company
    r'''HOVNA|OVNANIAN|HOVANA|HOVANI|HOVNI|
        H0VNANIAN|HANANIAN|HAVAANIAN|HAVANIAN|HAVNANIAN|HAVNONIAN|HAVONIAN|HEVNANIAN|HOBNANIAN|HONANIAN|
        HONVAN|HONVNA|HOUNANIAN|HOUVANIAN|HOVAANIAN|HOVANNIAN|HOVHANIAN|HOVINANIAN|HOVMANIAN|
        HOVNN|HOWNANIAN|HOYNANIAN|''': 'HOVNANIAN ENTERPRISES',
    r"SHEA H|SHEA-H|SHAE H": 'SHEA HOMES',
    r"PERRY H(O)?M|PERRYHOMES|PERRY-HOM": 'PERRY HOMES',
    r"LGI H|LGI-H|LGI I HOMES|LGIHOMES|LG1 HOMES|LGL H": 'LGI HOMES',
    r"MATTAM|MATTAN|MATTARNY|MATAMMY|MATAMY": 'MATTAMY HOMES U.S. GROUP',
    r"BEAZER H|BEAZER-H|BEAZER. H|BEAZOR|BEAZR": 'BEAZER HOMES',
    r"HIGHLAND H(O)?M|HIGHLAND-HOM": 'HIGHLAND HOMES',
    r"TANLEY MAR|TANLEY-MA": 'STANLEY MARTIN HOMES',
    r"^GL H": 'GL HOMES',
    r'''^CB J|SOUTHGATE HM|SOUTHGATE HOM|NORMANDY HOM|NORMANDY HM|CENTRE LIVING|
        PROVIDENCE GR|PROVIDENCE GP''': 'GREEN BRICK PARTNERS',  # https://greenbrickpartners.com/brands-services/;
    # TROPHY SIGNATURE HOMES(X), CB JENI, SOUTHGATE HOMES, NORMANDY HOMES, CENTRE LIVING HOMES, GHO HOMES(X), THE PROVIDENCE GROUP
    # r"DRB P": 'DRB ENTERPRISES',  # DRB GROUP seems to be a different company. currently only 1 in the data
    r"DREES H": 'DREES HOMES',
    r"FISCHER H(O)?M|FISCHERH": 'FISCHER HOMES',
    r"MINTO COM|MINTO B": 'MINTO COMMUNITIES USA',  # also MINTO BUILDERS, but other MINTO seems to be different
    r"BRIGHTLAND H|GEHAN H|GEHAM H|GEHEN H|GEHN H|GEHNA H": 'BRIGHTLAND HOMES',  # fka GEHAN HOMES
    r"BROOKFIELD RES": 'BROOKFIELD RESIDENTIAL PROPERTIES',
    r"BLOOMFIELD H|BLOOMFIELDH|BLOOMFIELDS H|BLOOMFIELS H|BLOOMFILED H|BLOOMFLELD H|BLOOMFTELD H": 'BLOOMFIELD HOMES',
    r"NEAL COM|NEAL CM": 'NEAL COMMUNITIES OF SOUTHWEST FLORIDA',
    r"^TRUE H(O)?M": 'TRUE HOMES',
    r"CHESMAR H|CHESMARH|CHESMAR-H|CHESMAR II|CHESMAR JOMES|CHESMAR T|CHESMER H": 'CHESMAR HOMES',
    r"FIRST T.*X.* H": 'FIRST TEXAS HOMES',
    r"^SDH|SMITH DOU": 'SMITH DOUGLAS HOMES',
    r"^POP H|CARTER HILL H|WESTOVER H|WESTOVER HOM": 'THE CHALLENGER GROUP',  # https://www.linkedin.com/company/the-challenger-group-inc/about/
    # PoP Homes, Casina Creek Homes(X), Carter Hill Homes, Westover Homes
    r"^EPCON|EPCOM": 'EPCON COMMUNITIES',
    r"HOMES BY WEST.*Y": 'HOMES BY WESTBAY',
    r"^CBH H": 'CBH HOMES',
    r"^LEGEND H(O)?M": 'LEGEND HOMES',
    r"TRUMARK|WATHEN CAST|MANGANO H": 'TRUMARK COMPANIES',  # the hierarchy is a bit complicated...; Wathen Castanos, Mangano
    r"EASTWOOD H(O)?M": 'EASTWOOD HOMES',
    r"^EDGE H(O)?M": 'EDGE HOMES',
    r"ROBSON COMM|^ROBSON H": 'ROBSON COMMUNITIES',
    r"^AMERICAN LEGE|AMERICAN LEGAND": 'AMERICAN LEGEND HOMES',
    # r"HARKINS BUILDERS": 'HARKINS BUILDERS',  # no record in data
    r"^IVORY H(O)?M": 'IVORY HOMES',
    r"CASTLEROCK COM": 'CASTLEROCK COMMUNITIES',
    r"WESTIN H|WESTIN-H": 'WESTIN HOMES',
    r"^STOCK DEV": 'STOCK DEVELOPMENT',
    r"^NEW HOME CO |^NEW HOME COM": 'NEW HOME COMPANY',
    # r"NexMetro Communities": 'NEXMETRO COMMUNITIES',  # no record in data
    r"HABITAT FOR HUM|HABITAT OF HUM": 'HABITAT FOR HUMANITY INTERNATIONAL',
    r"^PARK SQ.* H|^PARK SQ.* E": 'PARK SQUARE HOMES',
    r"^DAVIDSON H(O)?M": 'DAVIDSON HOMES',
    r"BETEN.* H": 'BETENBOUGH HOMES',
    r"SCHELL BR": 'SCHELL BROTHERS',
    r"LOMBARDO H.*M": 'LOMBARDO COMPANIES',
    r"^UNITED H.*M|^GREAT SOUTHERN H|GREAT SOUTHEN H|GREAT SOUTHER H|GREAT SOUTHERM H|GREAT SOUTHERNS H": 'UNITED HOMES GROUP',  # fka Great Southern Homes
    r"^GRAND H(O)?M|^GRAND HON": 'GRAND HOMES',
    r"DESERT VIEW H|ASPEN VIEW H|HORIZON VIEW H|ARMADILLO H|^VIEW HO": 'VIEW HOMES',  # https://www.linkedin.com/company/viewhomes/about/
    r"MCBRIDE .*SO|MCBRDE & SON H|MCBRIDE HOMES ": 'MCBRIDE & SON COMPANIES',
    r"SHADDOCK H|SHADDOCK-H": 'SHADDOCK HOMES',
    r"KEYSTONE CUSTO": 'KEYSTONE CUSTOM HOMES',
    r"HISTORY MA.*": 'HISTORYMAKER HOMES',
    r"VAN MET|VAN MATRE": 'VAN METRE COMPANIES',
    r"^VIERA B.*D": 'VIERA BUILDERS',
    r"TILSON .*H.*M": 'TILSON HOME',
    r"SUMMER( )?HILL H(O)?M": 'SUMMERHILL HOMES',
    # r"ALLEN EDWIN H": 'ALLEN EDWIN HOMES',  # no record in data
    r"^LANDON H": 'LANDON HOMES',
    r"^SIGNATURE H(O)?M": 'SIGNATURE HOMES',
    r"^WILLIAMS H(O)?M": 'WILLIAMS HOMES',
    r"HHHUN|HHHNUT|HHHUT|HHHUUNT": 'HHHUNT',
    r"NEWMARK H": 'NEWMARK HOMES',
    r"GALAXY B": 'GALAXY BUILDERS',
    r"CAVIN.* & C|CAVIN.* & G|CAVIN.* CA": 'CAVINESS & CATES COMMUNITIES',
    r"VISIONARY H(O)?M": 'VISIONARY HOMES',
    r"VISIONARY H(O)?M.* B": 'VISIONARY HOME BUILDERS',  # NOTE: just to differentiate with the above
    r"WILLIAM RY.* H|WILLIAMS RY.* H": 'WILLIAM RYAN HOMES',
    r"JOHN MOUR|JOHN MOUI|JOHN MUI|JOHN MUO|JOHN MUR": 'JOHN MOURIER CONSTRUCTION',
    r"HAKES BR": 'HAKES BROTHERS',
    r"^RIVERSIDE HO.*B": 'RIVERSIDE HOMEBUILDERS',
    r"^ELLIOT H|^ELLIOTT H(O)?M": 'ELLIOTT HOMES',
    r"^STONE MARI|^STONE MART|^STONE-MAR": 'STONE MARTIN BUILDERS',
    # r"TRADITIONS OF AMERICA": 'TRADITIONS OF AMERICA',  # no record in data
    r"MAIN STREET H(O)?M": 'MAIN STREET HOMES',
    r"ELI.* PROP.* AM|ELI.* PROP.*/ AM": 'ELITE PROPERTIES OF AMERICA',
    r"^EASTBROOK H": 'EASTBROOK HOMES',
    r"IMPRESSION H": 'IMPRESSION HOMES',
    r"SAN JOAQUIN VALLEY": 'SAN JOAQUIN VALLEY HOMES',
    # r"^EYA": 'EYA',  # no record in data
    r"STYLEC.* B": 'STYLECRAFT BUILDERS',
    r"^FULTON H(O)?M": 'FULTON HOMES',
    r"^WPG ": 'WOODBRIDGE PACIFIC GROUP',
    r"CARUSO H": 'CARUSO HOMES',
    # r"BBL": 'BBL BUILDING COMPANY', # no record in data
    r"^SITTERLE H|^SITTERIE H": 'SITTERLE HOMES',
    r"OLE SO.* P|OLE SOUTHPR": 'OLE SOUTH PROPERTIES',
    r"HOMES BY TABER": 'HOMES BY TABER',
    r"^EVERMORE H|EVERMOOR HOMES|AMERICAN SOUTHERN H": 'EVERMORE HOMES',  # fka American Southern Homes
    # r"CARUSO H": 'EVERGREENE HOMES',  # no record in data
    r"HOMES BY DIC": 'HOMES BY DICKERSON',
    r"^LEGACY H(O)?M": 'LEGACY HOMES',
    r"^LEGACY H.*BU|^LEGACY H.*BL": 'LEGACY HOME BUILDERS',  # NOTE: just to differentiate with the above
    r"BLANDFORD H": 'BLANDFORD HOMES',
    # r"TCB": 'THE COMMUNITY BUILDERS',  # hard to indify unique name
    r"BEECHWOOD HO": 'BEECHWOOD HOMES',  # NOTE: but this has too few records
    r"BERKS H(O)?M": 'BERKS HOMES',
    r"REGENT H(O)?M": 'REGENT HOMES',
    r"^LANDMARK H(O)?M": 'LANDMARK HOMES',
    # r"REGENT H(O)?M": 'HOME CREATIONS',  # claim to be largest in Oklahoma, but can't find record in data
    r"TRATON H|TRATON C.*P": 'TRATON HOMES',
    r"LANDMARK 24": 'LANDMARK 24 HOMES',
    r"^SK B": 'SK BUILDERS',
    # r"LECESSE": 'LECESSE DEVELOPMENT',  # no record in data
    r"ESPERANZA H(O)?M|ESPERANZA H M": 'ESPERANZA HOMES',
    r"SEAGATE H(O)?M": 'SEAGATE HOMES',
    r"^ANGLIA H|^ANGLIA-H": 'ANGLIA HOMES',
    r"NILSON H|NILSSON H": 'NILSON HOMES',
    r"VALOR COM": 'VALOR COMMUNITIES',
    r"CRAFTMARK H": 'CRAFTMARK HOMES',
    r"HANSON BL|HANSON BU": 'HANSON BUILDERS',
    r"GRANITE R.DGE B": 'GRANITE RIDGE BUILDERS',
    r"PRATT HOME.*B": 'PRATT HOME BUILDERS',
    r"CAPSTONE H(O)?M|CAPSTONE-H|CAPSTONE HI": 'CAPSTONE HOMES',
    r"UNITED BUILT H|UNITED BILT H|UNITED-B": 'UNITED BUILT HOMES',
    r"^IDEAL HM|^IDEAL HOM": 'IDEAL HOMES & NEIGHBORHOODS',
    # r"GRAND OAK": 'GRAND OAK BUILDERS',  # https://grandoakbuilders.com/builders/
    r"DON JU.* B|NEW MARK H": 'DON JULIAN BUILDERS AND NEW MARK HOMES-KC',
    r"NEW TRADITION H|NEW TRADIDTION H|NEW TRADION H|NEW TRADITIION H": 'NEW TRADITION HOMES',
    r"CHARTER HM|CHARTER HOM": 'CHARTER HOMES & NEIGHBORHOODS',
    # r"COLINA HOMES": 'COLINA HOMES',  # no record in data
    r'''SOUTHERN HOMES OF MI|SOUTHERN HOMES MI|SOUTHERN HOMES OF PA|SOUTHERN HOMES OF PO|
        SOUTHERN HOMES PO|SOUTHERN HOMES PA|SOUTHERN HOMES OF BRO|SOUTHERN HOMES BRO|
        SOUTHERN HOMES OF DAV|SOUTHERN HOMES DAV|SOUTHERN HOMES OF EST|SOUTHERN HOMES EST|
        SOUTHERN HOMES OF FL|SOUTHERN HOMES FL''': 'SOUTHERN HOMES',  # NOTE: there are too many SOUTHERN HOMES, so play it safe
    r"FIRST AM.* H": 'FIRST AMERICA HOMES',  # (a Signorelli Company)
    r"BOISE HUNTER H": 'BOISE HUNTER HOMES',
    r"WINDSOR H(O)?M": 'WINDSOR HOMES',  # NOTE: this one is based in North Carolina (but there's a same name in Fort Wayne, IN)
    r"^WINDSOR H.* CU": 'WINDSOR HOMES TEXAS',
    r"TIM O.*BRIEN|TIM OBRIAN|TIM ORIEN": "TIM O'BRIEN HOMES",
    r"MCKEE H": 'MCKEE HOMES',
    r"^REGENCY H(O)?M": 'REGENCY HOMES',  # NOTE: just to... This one is uncertain... (a lot in data, but few online info)
    r"^REGENCY H(O)?M.*LD": 'REGENCY HOMEBUILDERS',
    r"PIEDMONT RES": 'PIEDMONT RESIDENTIAL',
    r"^RODROCK H": 'RODROCK HOMES',
    r"TRESIDIO H|TRESIDIO CORP": 'TRESIDIO HOMES',
    r"^KEYSTONE H(O)?M": 'KEYSTONE GROUP',  # dba Keystone Homes. NOTE: too many keystone homes...
    r"^KEYSTONE H(O)?M.*LD": 'KEYSTONE HOMEBUILDERS',  # NOTE: just to differentiate with the above
    r"^KEYSTONE CONS": 'KEYSTONE CONSTRUCTION LLC',
    r"STYLECRAFT H": 'STYLECRAFT HOMES',
    r"HUNTER Q|HUNTER-Q": 'HUNTER QUINN HOMES',
    r"^CLASSICA H": 'CLASSICA HOMES',
    r"^CREATIVE H(O)?M(E)?S": 'CREATIVE HOMES',  # too many creative homes...
    r"JW COLLECTION": 'JW COLLECTION',  # NOTE: luxury. no record in data.
    r"^NAPOLITANO": 'NAPOLITANO HOMES',  # not sure, but hard to distinguish so collect all
    r"^INSIGHT H": 'INSIGHT HOMES',
    r"BAESSLER H|BAESS.* RES|BAESS.* CON": 'BAESSLER HOMES',
    r"PEACHTREE B.* G|^SILVERSTONE RE": 'PEACHTREE BUILDING GROUP',  # fka Silverstone Communities
    r"WARMI.* ASS|WARMI[^ ]* HOM": 'THE WARMINGTON GROUP',
    r"BUFFINGTON H[^ ]* OF AR|BUFFINGTON HOMES INC": 'BUFFINGTON HOMES OF ARKANSAS',  # actually the INC name is not certain
    r"GEMCR[^ ]* H|GEMCFRAFT HOM|GEMCFAFT HOM|GEMCCRAFT HOM|GEMCARAFT HOM": 'GEMCRAFT HOMES',
    r"RIZ COM": 'RIZ COMMUNITIES AND DEVELOPMENT',
    # r"AHV": 'AMERICAN HOUSING VENTURES',  # they say they mainly build for rent.
    r"^WOODLAND H.* HUN": 'WOODLAND HOMES OF HUNTSVILLE',  # NOTE: also a lot of woodland homes online, and many in data as well
    r"PROVIDENCE HOMES INC": 'PROVIDENCE HOMES',  # there are too many Providence Homes online, so here we pick one that has clear definition and with many records
    r"^WORMALD DEV": 'WORMALD',
    r"^LINCOLN PROP": 'WILLOW BRIDGE PROPERTY COMPANY',  # fka Lincoln Property Co.
    r"^MANUEL B": 'MANUEL BUILDERS',
    r"BILL B.*Z[^ ]* H": 'BILL BEAZLEY HOMES',
    r"AMERICAN CLASSIC H": 'AMERICAN CLASSIC HOMES',
    r"^UNIVERSAL H(O)?M(E)?S LLC": 'UNIVERSAL HOMES',  # there's another INC one, not sure if the same
    r"MCGUINN HO": 'MCGUINN HYBRID HOMES',
    # r"BALDWIN & S": 'BALDWIN & SONS',  # no record in data, but seems to be a big company
    r"STEPPI[^ ]*STONE|STEPPI[^ ]* STONE": 'STEPPING STONE HOMES',
    r"^W[\. ]*B[\. ]* HOME|W.B.HOMES": 'W.B. HOMES',
    r"EDWARDS H(O)?M": 'EDWARDS HOMES',  # NOTE: just to collect the rest of edwards homes
    r"EDWARDS H[^ ]* .*PAS|EDWARDS HMS OF NEW MEXICO LLC": 'EDWARDS HOMES OF EL PASO',
    r"^MILLER[ ]*&[ ]*SM|^MILLER AND SM": 'MILLER & SMITH',
    r"^PYAT[^ ]* B": 'PYATT BUILDERS',
    r"^MARRANO.*MARC": 'MARRANO HOMES',
    r"^ENCE": 'ENCE HOMES',
    r"^TIM LEWIS": 'TIM LEWIS COMMUNITIES',
    r"^GENTRY H(O)?M": 'GENTRY HOMES',
    r"^SUMEER H": 'SUMEER HOMES',
    r"^JAMES E[^ ]* CU|JAMES ENGLE H": 'JAMES ENGLE CUSTOM HOMES',
    r"^PACIFIC COM[^ ]* B": 'PACIFIC COMMUNITIES BUILDER',
    r"FLINTROCK": 'FLINTROCK BUILDERS',
    r"^ROBERTSON H": 'ROBERTSON HOMES',
    r"\bOLSON HOME(S)?\b(?!.*\bIM\w*\b)": 'THE OLSON COMPANY',
    r"MCCAF[^ ]* DEV|MCCAF[^ ]* G": 'MCCAFFREY HOMES',
    r"^HAMLET H(O)?M": 'HAMLET HOMES',
    r"^ROBUCK H(O)?M": 'ROBUCK HOMES',
    r"^TROPICAN": 'TROPICANA HOMES',
    r"^RIVERWOOD HOM": 'RIVERWOOD HOMES',
    r"^MAGNOLIA H(O)?M": 'MAGNOLIA HOMES',
    r"^TWILIGHT H(O)?M": 'TWILIGHT HOMES',
    r"^IVEY H(O)?M|^IVEY RE": 'IVEY HOMES',
    r"MASTERC[^ ]* B[^ ]* G": 'MASTERCRAFT BUILDER GROUP',
    r"^NEWCASTLE H(O)?M": 'NEWCASTLE HOMES',
    r"^VANTAGE H(O)?M": 'VANTAGE HOMES',
    r"^HOLLAND H(O)?M": 'HOLLAND HOMES',
    r"JP BROOKS": 'JP BROOKS BUILDERS',
    r"^FORIN[^ ]*": 'FORINO COMPANY',
    # r"REYNOLDS CONST|REYNOLDS HOMES|REYNOLDS DEV": 'REYNOLDS COMPANIES',  # not sure which, so ignore
    r"^PALO VERDE H(O)?M": 'PALO VERDE HOMES',
    r"^LOWDE[^ ]* NEW H": 'LOWDER NEW HOMES',
    r"^EDWARD ROSE": 'EDWARD ROSE BUILDING ENTERPRISE',
    r"^COVINGTON H(O)?M": 'COVINGTON HOMES',
    r"^SILVERTH[^ ]* H(O)?M|^SILVERTH[^ ]* DEV": 'SILVERTHORNE HOMEBUILDERS',
    r"^KENDALL H(O)?M": 'KENDALL HOMES',
    r"^AWK|KENT HOMES & ASSOC": 'AWK',  # dba Kent Homes & Associates
    r"^FRENCH BROT": 'FRENCH BROTHERS',
    r"^WINDSONG PR": 'WINDSONG PROPERTIES',
    r"WARD COMMU": 'WARD COMMUNITIES',
    # r"^INB": 'INB HOMES',  # should be a large new home builder, but no record in data
    r"^MCKINNEY B|^MCKINNEY & SON": 'MCKINNEY BUILDERS',
    r"^CLEARVIEW H(O)?M": 'CLEARVIEW HOMES',
    r"^KEY LAN(.)( )?H": 'KEY LAND HOMES',
    r"^GOODWYN BU": 'GOODWYN BUILDING COMPANY',
    r"^VENTURA H(O)?M": 'VENTURA HOMES',
    r"^THRIVE HOME B|^GREENTREE H|NEW TOWN B[^ ]*L|": 'THRIVE HOME BUILDERS',  # https://www.linkedin.com/company/thrive-home-builders/about/
    r"^MCARTHUR H(O)?M": 'MCARTHUR HOMES',
    r"^SANDCASTLE H(O)?M": 'SANDCASTLE HOMES',
    r"^SUNRIVER .*DEV": 'SUNRIVER ST GEORGE DEVELOPMENT',
    r"THE BUILDERS G|TBG DEV": 'THE BUILDERS GROUP',  # actually not so sure
    # r"SEKISUI": 'SEKISUI HOUSE',  # Sekisui House, large in Japan, owns MDC Holdings, but no record in data
    r"RAU[^ ]*[ |-]*COL": 'RAUSCH COLEMAN HOMES',
    r"^DRB GR": 'DRB GROUP',
    r"^DSL.(.)? H": 'DSLD HOMES',
    r"^ADAMS H(?!OLD|OR)|^ADAM H(O)?M[^ ]* .*FL": 'ADAMS HOMES',
    r"^THE VILLAGES .*LA": 'THE VILLAGES OF LAKE SUMTER',
    r"^MARON[^ ]+( |-)?H(.)?M": 'MARONDA HOMES',
    r"^AMERICAN H(.)?M": 'AMH',  # American Homes 4 Rent
    r"^LANDSEA": 'LANDSEA HOMES',
    r"^HOLIDAY B[^ ]*[L|D]": 'HOLIDAY BUILDERS',
    r"^KOLTER": 'THE KOLTER GROUP',
    r"^HAYDEN H": 'HAYDEN HOMES',
    # r"EMPIRE COMM": 'EMPIRE COMMUNITIES',  # There are a lot of Empire related names...
    r"^LONG LAKE$|LONG LAKE(S)? LTD|LONG LAKE(S)? LLC": 'LONG LAKE LTD',
    r"^PROMINENCE H": 'PROMINENCE HOMES',
    r"^SARATOGA H(.)?M": 'SARATOGA HOMES',
    r"^PACESETTER H": 'PACESETTER HOMES',
    r"SCHUB[^ ]* MI": 'SCHUBER MITCHELL HOMES',
    r"^MEGATEL H[^ ]*M": 'MEGATEL HOMES',
    r"ON TOP OF THE WORLD": 'ON TOP OF THE WORLD',  # no idea how to search for this...
    r"^ICI ": 'ICI HOMES',
    r"THE NEW HOME": 'THE NEW HOME COMPANY',
    # r"CHRISTOPHER ALAN H": 'CHRISTOPHER ALAN HOMES',  # no idea how to search for this...
    r"^BALL H(.)?M": 'BALL HOMES',
    r"^OLT[^ ]*F[ |-]?H": 'OLTHOF HOMES',
    r"^CHESAPEAKE H(.)?M": 'CHESAPEAKE HOMES',
    # r"ROCKHAVEN H": 'ROCKHAVEN HOMES',  # no record in data
    r"BILL CLA(?![^ ]*YTON)[^ ]*( )?H": 'BILL CLARK HOMES',
    r"SCOTT FE[^ ]*": 'SCOTT FELDER HOMES',
    r"MILES.* C(.)?M[^ ]*( |-)?[B|M]|^MILEST[^ ]* B[^ ]*R": 'MILESTONE COMMUNITY BUILDERS',
    r"^MCKINLEY H(.)[M|N]": 'MCKINLEY HOMES',
    r"TOUCHSTONE LIVING": 'TOUCHSTONE LIVING',
    r"^LIBERTY C(O)?M": 'LIBERTY COMMUNITIES',
    r"^CC H(O)?M": 'CC HOMES',
    r"^LOGAN H(O)?M": 'LOGAN HOMES',
    # r"BRIDGE TOWER": 'BRIDGE TOWER HOMES',  # no record in data
    r"^HUBBLE H(.)?M": 'HUBBLE HOMES',
    r"^LOKAL ..(.)? |LOKAL COM|LOKAL H(.)M": 'LOKAL HOMES',
    r"^A(\.)? SYD": 'A. SYDES CONSTRUCTION',
    r"^NATIONAL H(.)?M(.)? CORP": 'NATIONAL HOME CORPORATION',
    r"^CITY VEN": 'CITY VENTURES',
    r"^BRITE H(.)?M": 'BRITE HOMES',
    r"^OUR COUNTRY H": 'OUR COUNTRY HOMES',
    r"^GREENLAND H": 'GREENLAND HOMES',
    r"^RELIANT H": 'RELIANT HOMES',
    r"^GARMAN H": 'GARMAN HOMES',
    r"^PARTNERS IN B": 'PARTNERS IN BUILDING',
    r"^LIBERTY H(.)?M[^ ]*( )?B": 'LIBERTY HOME BUILDERS',
    r"^ANGLE H": 'ANGLE HOMES',
    r"^MHC (P|O)F G|^MHC GEORGIA": 'MY HOME COMMUNITIES',
    r"^MAIN[V|Y]U": 'MAINVUE HOMES',
    r"^SIMMONS H(.)?[M|N]": 'SIMMONS HOMES',
    r"DISCOVERY H(.)?M": 'DISCOVERY HOMES',
    r"BONAD[A|E]L": 'BONADELLE NEIGHBORHOODS',
    r"^GREENSTONE": 'GREENSTONE HOMES',
    # r"BEN STOUT CON": 'BEN STOUT CONSTRUCTION',  # no record in data
    r"SANDLIN H": 'SANDLIN HOMES',
    r"^FRONTIER COM": 'FRONTIER COMMUNITIES',  # have const, builders, custom homes, dev...
    r"^MEDALLION H": 'MEDALLION HOMES',
    r"^TURNER H(.)?M": 'TURNER HOMES'
}

builders_list_2 = {
    r"IMPRESSIONIST HM|IMPRESSIONIST HOM": 'IMPRESSIONIST HOMES',
    r"CARUSO B": 'CARUSO BUILDERS',
    r"^UNITED BLDR|^UNITED BUILDER": 'UNITED BUILDERS',
    r"SOUTHERN HOME BL|SOUTHERN HOME BU": 'SOUTHERN HOME BUILDERS',
    r"SOUTHERN HOMEBU": 'SOUTHERN HOMEBUILDERS',
    r"^WINDSOR INV": 'WINDSOR INVESTMENTS',
    r"TRESGER CONST": 'TRESGER CONSTRUCTION',
    r"^JOHN WIE|^JOHN WEI|^JOHN WEE|^JOHN WELA|^JOHN WIC|^JOHN WID": 'JOHN WIELAND HOMES',  # belongs to PULTEGROUP
    r"INSIGHT B.*LD.*G": 'INSIGHT BUILDING CO',
    r"INSIGHT BU.*RS": 'INSIGHT BUILDERS',
    r"^PEACHTREE CM|^PEACHTREE COM": 'PEACHTREE COMMUNITIES',
    r"^PEACHTREE CONST": 'PEACHTREE CONSTRUCTION',
    r"^PEACHTREE HM|^PEACHTREE HOM": 'PEACHTREE HOMES',
    r"^PEACHTREE RES": 'PEACHTREE RESIDENTIAL PROPERTIES',
    r"^PEACHTREE T.* COM|^PEACHTREE T.* CM": 'PEACHTREE TOWNHOME COMMUNITIES',
    r"BUFFINGTON H[^ ]* .*SAN|BUFFINGTON H[^ ]*.SAN": 'BUFFINGTON HOMES OF SAN ANTONIO',
    r"^PROVIDENCE H.*REG": 'PROVIDENCE HOMES OF REGENCY',
    r"^WORMALD H": 'WORMALD HOMES',
    r"^WORMALD C": 'WORMALD CONDOMINIUM',
    r"AMERICAN CLASSIC B": 'AMERICAN CLASSIC BUILDERS',
    r"^UNIVERSAL H[^ ]* B": 'UNIVERSAL HOMEBUILDERS',
    r"BALDWIN BUI": 'BALDWIN BUILDERS',
    r"^CHESAPEAKE HOLDINGS": 'CHESAPEAKE HOLDINGS',
    r"^GARMAN B": 'GARMAN BUILDERS',
    r"^LIBERTY H[^ ]*R( )?N": 'LIBERTY HARBOR NORTH',

}


# [This list need to be match for several times]
# The values in this list would be replaced with empty string
# => so actually the values are not needed, when the patterns are matched,
#    the string would be removed.
firm_structure_abbrv = [
    (r"\bL( )?L( )?[VC]\b|\bL( )?L( )?L( )?[VC]\b", 'LLC'),
    (r"\bL( )?T( )?(D)?\b", 'LTD'),
    (r"\bL( )?L( )?L( )?P\b", 'LLLP'),
    (r"\bL( )?L( )?P\b", 'LLP'),
    (r"\bL( )?P\b", 'LP'),
    (r"\bL( )?L\b", 'LL'),
    (r"\bC( )?O\b|\bCOMPANY\b", 'CO'),
    (r"\b(?:I( )?N( )?C|INCO(R)?POR[^ ]*)\b", 'INC'),
    (r"\bI( )?N\b", 'IN'),
    (r"JOIN(T)? VEN", 'JV'),
    (r"(?<=.)\bVEN(T)?(U)?(R)?(E)?(S)?\b", 'VEN'),  # venture(s)
    (r"(?<=.)\bGENERAL P[^ ]*\b", 'GP'),  # general partnership(s)
]
# TODO: the above format seems would only execute once for each row, need to
#       make it able to remove all matching patterns

# The states and Roman numerals need to be removed.
state_patterns = [
    (r"(?<=.)\b(?:AL(A)?BA(MA)?|AL(A)?)\b", 'AL'),
    (r"(?<=.)\b(?:ALASKA|AK)\b", 'AK'),
    (r"(?<=.)\b(?:ARIZONA|AZ)\b", 'AZ'),
    (r"(?<=.)\b(?:ARKANSAS|AR)\b", 'AR'),
    (r"(?<=.)\b(?:CALIFORNIA|CA)\b", 'CA'),
    (r"(?<=.)\b(?:COLORADO|CO)\b", 'CO'),
    (r"(?<=.)\b(?:CONNECTICUT|CT)\b", 'CT'),
    (r"(?<=.)\b(?:DELAWARE|DE)\b", 'DE'),
    (r"(?<=.)\b(?:FLORIDA|FL)\b", 'FL'),
    (r"(?<=.)\b(?:GEORGIA|GA)\b", 'GA'),
    (r"(?<=.)\b(?:HAWAII|HI)\b", 'HI'),
    (r"(?<=.)\b(?:IDAHO|ID)\b", 'ID'),
    (r"(?<=.)\b(?:ILLINOIS|IL)\b", 'IL'),
    (r"(?<=.)\b(?:INDIANA|IN)\b", 'IN'),
    (r"(?<=.)\b(?:IOWA|IA)\b", 'IA'),
    (r"(?<=.)\b(?:KANSAS|KS)\b", 'KS'),
    (r"(?<=.)\b(?:KENTUCKY|KY)\b", 'KY'),
    (r"(?<=.)\b(?:LOUISIANA|LA)\b", 'LA'),
    (r"(?<=.)\b(?:MAINE|ME)\b", 'ME'),
    (r"(?<=.)\b(?:MARYLAND|MD)\b", 'MD'),
    (r"(?<=.)\b(?:MASSACHUSETTS|MA)\b", 'MA'),
    (r"(?<=.)\b(?:MICHIGAN|MI)\b", 'MI'),
    (r"(?<=.)\b(?:MINNESOTA|MN)\b", 'MN'),
    (r"(?<=.)\b(?:MISSISSIPPI|MS)\b", 'MS'),
    (r"(?<=.)\b(?:MISSOURI|MO)\b", 'MO'),
    (r"(?<=.)\b(?:MONTANA|MT)\b", 'MT'),
    (r"(?<=.)\b(?:NEBRASKA|NE)\b", 'NE'),
    (r"(?<=.)\b(?:NEVADA|NV)\b", 'NV'),
    (r"(?<=.)\b(?:NEW HAMPSHIRE|NH)\b", 'NH'),
    (r"(?<=.)\b(?:NEW JERSEY|NJ)\b", 'NJ'),
    (r"(?<=.)\b(?:NEW MEXICO|NM)\b", 'NM'),
    (r"(?<=.)\b(?:NEW YORK|NY)\b", 'NY'),
    (r"(?<=.)\b(?:NORTH CAROLINA|NC)\b", 'NC'),
    (r"(?<=.)\b(?:NORTH DAKOTA|ND)\b", 'ND'),
    (r"(?<=.)\b(?:OHIO|OH)\b", 'OH'),
    (r"(?<=.)\b(?:OKLAHOMA|OK)\b", 'OK'),
    (r"(?<=.)\b(?:OREGON|OR)\b", 'OR'),
    (r"(?<=.)\b(?:PENNSYLVANIA|PA)\b", 'PA'),
    (r"(?<=.)\b(?:RHODE ISLAND|RI)\b", 'RI'),
    (r"(?<=.)\b(?:SOUTH CAROLINA|SC)\b", 'SC'),
    (r"(?<=.)\b(?:SOUTH DAKOTA|SD)\b", 'SD'),
    (r"(?<=.)\b(?:TENNESSEE|TN)\b", 'TN'),
    (r"(?<=.)\b(?:TEXAS|TX)\b", 'TX'),
    (r"(?<=.)\b(?:UTAH|UT)\b", 'UT'),
    (r"(?<=.)\b(?:VERMONT|VT)\b", 'VT'),
    (r"(?<=.)\b(?:VIRGINIA|VA)\b", 'VA'),
    (r"(?<=.)\b(?:WASHINGTON|WA)\b", 'WA'),
    (r"(?<=.)\b(?:WEST VIRGINIA|WV)\b", 'WV'),
    (r"(?<=.)\b(?:WISCONSIN|WI)\b", 'WI'),
    (r"(?<=.)\b(?:WYOMING|WY)\b", 'WY')
]


# 1 ~ 90
roman_numeral_patterns = [
    (r"(?<=.)\bI\b", 'I'),
    (r"(?<=.)\bI( )?I\b", 'II'),
    (r"(?<=.)\bI( )?I( )?I\b", 'III'),
    (r"(?<=.)\bI( )?V\b", 'IV'),
    (r"(?<=.)\bV\b", 'V'),
    (r"(?<=.)\bV( )?I\b", 'VI'),
    (r"(?<=.)\bV( )?I( )?I\b", 'VII'),
    (r"(?<=.)\bV( )?I( )?I( )?I\b", 'VIII'),
    (r"(?<=.)\bI( )?X\b", 'IX'),
    (r"(?<=.)\bX\b", 'X'),
    (r"(?<=.)\bX( )?I\b", 'XI'),
    (r"(?<=.)\bX( )?I( )?I\b", 'XII'),
    (r"(?<=.)\bX( )?I( )?I( )?I\b", 'XIII'),
    (r"(?<=.)\bX( )?I( )?V\b", 'XIV'),
    (r"(?<=.)\bX( )?V\b", 'XV'),
    (r"(?<=.)\bX( )?V( )?I\b", 'XVI'),
    (r"(?<=.)\bX( )?V( )?I( )?I\b", 'XVII'),
    (r"(?<=.)\bX( )?V( )?I( )?I( )?I\b", 'XVIII'),
    (r"(?<=.)\bX( )?I( )?X\b", 'XIX'),
    (r"(?<=.)\bX( )?X\b", 'XX'),
    (r"(?<=.)\bX( )?X( )?I\b", 'XXI'),
    (r"(?<=.)\bX( )?X( )?I( )?I\b", 'XXII'),
    (r"(?<=.)\bX( )?X( )?I( )?I( )?I\b", 'XXIII'),
    (r"(?<=.)\bX( )?X( )?I( )?V\b", 'XXIV'),
    (r"(?<=.)\bX( )?X( )?V\b", 'XXV'),
    (r"(?<=.)\bX( )?X( )?V( )?I\b", 'XXVI'),
    (r"(?<=.)\bX( )?X( )?V( )?I( )?I\b", 'XXVII'),
    (r"(?<=.)\bX( )?X( )?V( )?I( )?I( )?I\b", 'XXVIII'),
    (r"(?<=.)\bX( )?X( )?I( )?X\b", 'XXIX'),
    (r"(?<=.)\bX( )?X( )?X\b", 'XXX'),
    (r"(?<=.)\bX( )?X( )?X( )?I\b", 'XXXI'),
    (r"(?<=.)\bX( )?X( )?X( )?I( )?I\b", 'XXXII'),
    (r"(?<=.)\bX( )?X( )?X( )?I( )?I( )?I\b", 'XXXIII'),
    (r"(?<=.)\bX( )?X( )?X( )?I( )?V\b", 'XXXIV'),
    (r"(?<=.)\bX( )?X( )?X( )?V\b", 'XXXV'),
    (r"(?<=.)\bX( )?X( )?X( )?V( )?I\b", 'XXXVI'),
    (r"(?<=.)\bX( )?X( )?X( )?V( )?I( )?I\b", 'XXXVII'),
    (r"(?<=.)\bX( )?X( )?X( )?V( )?I( )?I( )?I\b", 'XXXVIII'),
    (r"(?<=.)\bX( )?X( )?X( )?I( )?X\b", 'XXXIX'),
    (r"(?<=.)\bX( )?L\b", 'XL'),
    (r"(?<=.)\bX( )?L( )?I\b", 'XLI'),
    (r"(?<=.)\bX( )?L( )?I( )?I\b", 'XLII'),
    (r"(?<=.)\bX( )?L( )?I( )?I( )?I\b", 'XLIII'),
    (r"(?<=.)\bX( )?L( )?I( )?V\b", 'XLIV'),
    (r"(?<=.)\bX( )?L( )?V\b", 'XLV'),
    (r"(?<=.)\bX( )?L( )?V( )?I\b", 'XLVI'),
    (r"(?<=.)\bX( )?L( )?V( )?I( )?I\b", 'XLVII'),
    (r"(?<=.)\bX( )?L( )?V( )?I( )?I( )?I\b", 'XLVIII'),
    (r"(?<=.)\bX( )?L( )?I( )?X\b", 'XLIX'),
    (r"(?<=.)\bL\b", 'L'),
    (r"(?<=.)\bL( )?I\b", 'LI'),
    (r"(?<=.)\bL( )?I( )?I\b", 'LII'),
    (r"(?<=.)\bL( )?I( )?I( )?I\b", 'LIII'),
    (r"(?<=.)\bL( )?I( )?V\b", 'LIV'),
    (r"(?<=.)\bL( )?V\b", 'LV'),
    (r"(?<=.)\bL( )?V( )?I\b", 'LVI'),
    (r"(?<=.)\bL( )?V( )?I( )?I\b", 'LVII'),
    (r"(?<=.)\bL( )?V( )?I( )?I( )?I\b", 'LVIII'),
    (r"(?<=.)\bL( )?I( )?X\b", 'LIX'),
    (r"(?<=.)\bL( )?X\b", 'LX'),
    (r"(?<=.)\bL( )?X( )?I\b", 'LXI'),
    (r"(?<=.)\bL( )?X( )?I( )?I\b", 'LXII'),
    (r"(?<=.)\bL( )?X( )?I( )?I( )?I\b", 'LXIII'),
    (r"(?<=.)\bL( )?X( )?I( )?V\b", 'LXIV'),
    (r"(?<=.)\bL( )?X( )?V\b", 'LXV'),
    (r"(?<=.)\bL( )?X( )?V( )?I\b", 'LXVI'),
    (r"(?<=.)\bL( )?X( )?V( )?I( )?I\b", 'LXVII'),
    (r"(?<=.)\bL( )?X( )?V( )?I( )?I( )?I\b", 'LXVIII'),
    (r"(?<=.)\bL( )?X( )?I( )?X\b", 'LXIX'),
    (r"(?<=.)\bL( )?X( )?X\b", 'LXX'),
    (r"(?<=.)\bL( )?X( )?X( )?I\b", 'LXXI'),
    (r"(?<=.)\bL( )?X( )?X( )?I( )?I\b", 'LXXII'),
    (r"(?<=.)\bL( )?X( )?X( )?I( )?I( )?I\b", 'LXXIII'),
    (r"(?<=.)\bL( )?X( )?X( )?I( )?V\b", 'LXXIV'),
    (r"(?<=.)\bL( )?X( )?X( )?V\b", 'LXXV'),
    (r"(?<=.)\bL( )?X( )?X( )?V( )?I\b", 'LXXVI'),
    (r"(?<=.)\bL( )?X( )?X( )?V( )?I( )?I\b", 'LXXVII'),
    (r"(?<=.)\bL( )?X( )?X( )?V( )?I( )?I( )?I\b", 'LXXVIII'),
    (r"(?<=.)\bL( )?X( )?X( )?I( )?X\b", 'LXXIX'),
    (r"(?<=.)\bL( )?X( )?X( )?X\b", 'LXXX'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?I\b", 'LXXXI'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?I( )?I\b", 'LXXXII'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?I( )?I( )?I\b", 'LXXXIII'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?I( )?V\b", 'LXXXIV'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?V\b", 'LXXXV'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?V( )?I\b", 'LXXXVI'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?V( )?I( )?I\b", 'LXXXVII'),
    (r"(?<=.)\bL( )?X( )?X( )?X( )?V( )?I( )?I( )?I\b", 'LXXXVIII'),
    (r"(?<=.)\bL( )?X( )?X( )?I( )?X\b", 'LXXXIX'),
    (r"(?<=.)\bX( )?C\b", 'XC')
]


# [This list need to be match only once]
general_abbrv = [
    (r"\bLN\b", 'LANE'),
    (r"\bAVENU(E)?\b", 'AVE'),
    (r"\bF(A)?M(I)?LY\b", 'FAM'),
    (r"\bH(.)?(M|RN|TN)(E)?( )?B[^ ]*\b", 'HMBLDR'),
    (r"\b(?:[HJGB]OMES|IIOMES|I[ -]IOMES|HM(E)?S|H.ME)\b", 'HM'),
    (r"\bRES(I)?D[^ C]*\b", 'RESID'),
    (r"\bDEV[^ ]*\b", 'DEV'),  # development(s), developer(s)
    (r"\b(?:ASSOC(S)?|ASSOCIAT(E)?(S)?|ASS(O)?[^ ]*N(S)?)\b", 'ASSOC'),  # associate(s), association(s)
    (r"\b(?:CROP|CORP[^ ]*)\b", 'CORP'),  # corporation(s), corporate(s)
    (r"\bINV[^ ]*\b", 'INV'),  # investment(s), investor(s)
    (r"\bB[^ ]*LD(.)?R(S)?\b", 'BLDR'),  # builder(s)
    (r"\bH(O)?LD[^ ]*G(S)?\b", 'HLDG'),  # holding(s)
    (r"\bB[^ ]*LD[^ ]*G(S)?\b", 'BLDG'),  # building(s)
    (r"\bPROP[^ ]*\b", 'PROP'),  # property(ies)
    (r"\b(?:COMMU[^ ]*|COMMN[^ ]*(Y|ES)|C[MN](N)?T(Y)?(S)?|COMT[^ ]*)\b", 'CMNTY'),  # community(ies)
    (r"\bCOMM[NO][^ ]*\b|\bC(M)?MN(S)?\b", 'CMN'),  # common(s)
    (r"\bVALL(L|E|Y)?(EY)?\b|\bV(.)?LY\b", 'VLY'),  # valley(ies)
    (r"\bC(O)?ND(O)?[^ ]*\b", 'CONDO'),  # CONDOMINIUM(S), CONDO(S)
    (r"\bGR(OU)?P(S)?\b", 'GRP'),  # group(s)
    (r"\bP(A)?(R)?T(E)?N(E)?(R)?(S)?\b", 'PTNR'),  # PARTNERS(s)
    (r"\bP(A)?(R)?T(E)?N(E)?(R)?S(H)?(I)?P\b|\bPRNS[^ ]*\b", 'PTNSHP'),  # partnership(s)
    (r"\bCONST[^ ]*\b", 'CONST'),  # construction(s)
    (r"\bREAL(L)?(T)?(Y|IES)\b", 'RLTY'),  # realty(ies)
]

# Note that the order here matters! For similar names, the order is from specific to general.
builders_list = [
    (r'''D(\.| )?( )?R(\.| )?( )?(-)?\bH[^ KZSHMN]{4,7}(M|N)?\b|
         D(\.| )?( )?R(\.| )?( )?(-)?[HD]O[^ Z]*ON''', 'D.R. HORTON'),
    (r"LENNER|^LENAR|^LENNR|^LENNAT|^LNR LENNAR|LANNAR|LENNNAR|LENNAR", 'LENNAR'),
    (r"^PLUTE|^PAULTE|PULTE HM|^PULTE|PULTEGROUP", 'PULTEGROUP'),  # centex?
    (r"^\bTO[LI][LI][ -]\b|^TOL |TOLLBRO", 'TOLL BROTHERS'),  # https://www.sec.gov/Archives/edgar/data/794170/000079417021000079/tol-20211031x10kxex22.htm
    (r"^N V R|NVR", 'NVR'),
    (r'''^TAYLOR MORRSION|TAYOR MORRISON|TAYLOR MARRISON|TAYLOR MORISON|TAYLOR MONISON|TM HOMES|TAYLOR MORRISON|
        ^MORRISON HM|^MORISON HOMES|MORISSON HOMES''', 'TAYLOR MORRISON'),
    (r"^KB H(O)?M|^KB H0MES|^KAUFMAN & B|^KUAFMAN & B|^KAUFMAN B|KB-HOME", 'KB HOME'),
    (r'''^MERITAE|^MERITAG|^MERITA.E HOMES|^MERITAQ|^MERITGAE|^MERITGAGE HOMES|^MERITAHE HOMES|
         ^MERITFAGE|^MERITGE|^MERJTAGE HOMES|MERITAGE HOMES''', 'MERITAGE HOMES'),
    (r"RICHM.* AM|^RICHMONDAM|^RICHMOND MERICAN|^RICHO.* AM|RICHMOND AMERICAN", 'M.D.C. HOLDINGS'),
    (r"^CLAYTON HOM|^CLAYTON HM|CLAYTON PRO|^CLAYON PRO", 'CLAYTON PROPERTIES GROUP'),
    (r"M/I H(O)?M|^M/I . HOME|^M/I. HOME|^M/.HOME|^M/ .HOME|^M/. HOME|^M/HOME|^MI HM|^MI HOME", 'M/I HOMES'),
    (r'''^DREAM FINDER|^DFH CAPITOL|^DFH CLOVER|^DFH CORONA|^DFH GREYHAWK|^DFH JOHNS LANDING|^DFH LAND|
         ^DFH MANDARIN|^DFH WILDWOOD''', 'DREAM FINDERS HOMES'), # https://www.sec.gov/Archives/edgar/data/1825088/000114036121010794/brhc10022392_ex21-1.htm
    (r"^TRI POINT|^TRI POLNT|^TRI PONTE|TRI-POINT", 'TRI POINTE HOMES'),
    (r"CENTURY COM|CENTURYCOM", 'CENTURY COMMUNITIES'),
    (r"^ASHTON W|^HOMES ASHTON W|ASHTONW", 'ASHTON WOODS'),
    (r"^DAVID WEEK", 'DAVID WEEKLEY HOMES'),  # DW HOMES seems to be a different company
    (r'''H[Q0O][VYWB][MHN]A|H[Q0O][VYWB]A[MHN][AIN]| OVNANIAN|K HOVNI|
         ^(K )?H[A][VN]ANI|HAV[AN][AO]|HEVNANIAN|HONANIAN|HONV[AN]|
         ^(K )?HOU[NV]A|HOVAA|HOVI[ANI][NA]|HOVN[NM]''', 'HOVNANIAN ENTERPRISES'),
    (r"SHEA H|SHEA-H|SHAE H", 'SHEA HOMES'),
    (r"PERRY H(O)?M|PERRYHOMES|PERRY-HOM", 'PERRY HOMES'),
    (r"LGI H|LGI-H|LGI I HOMES|LGIHOMES|LG1 HOMES|LGL H", 'LGI HOMES'),
    (r"MATTAM|MATTAN|MATTARNY|MATAMMY|MATAMY", 'MATTAMY HOMES U.S. GROUP'),
    (r"BEAZER H|BEAZER-H|BEAZER. H|BEAZOR|BEAZR", 'BEAZER HOMES'),
    (r"HIGHLAND H(O)?M|HIGHLAND-HOM", 'HIGHLAND HOMES'),
    (r"TANLEY MAR|TANLEY-MA", 'STANLEY MARTIN HOMES'),
    (r"^GL H", 'GL HOMES'),
    (r'''^CB J|SOUTHGATE HM|SOUTHGATE HOM|NORMANDY HOM|NORMANDY HM|CENTRE LIVING|
         PROVIDENCE GR|PROVIDENCE GP''', 'GREEN BRICK PARTNERS'),  # https://greenbrickpartners.com/brands-services/;
    # TROPHY SIGNATURE HOMES(X), CB JENI, SOUTHGATE HOMES, NORMANDY HOMES, CENTRE LIVING HOMES, GHO HOMES(X), THE PROVIDENCE GROUP
    # (r"DRB P", 'DRB ENTERPRISES'),  # DRB GROUP seems to be a different company. currently only 1 in the data
    (r"DREES H", 'DREES HOMES'),
    (r"FISCHER H(O)?M|FISCHERH", 'FISCHER HOMES'),
    (r"MINTO COM|MINTO B", 'MINTO COMMUNITIES USA'),  # also MINTO BUILDERS, but other MINTO seems to be different
    (r"BRIGHTLAND H|GEHAN H|GEHAM H|GEHEN H|GEHN H|GEHNA H", 'BRIGHTLAND HOMES'),  # fka GEHAN HOMES
    (r"BROOKFIELD RES", 'BROOKFIELD RESIDENTIAL PROPERTIES'),
    (r"BLOOMFIELD H|BLOOMFIELDH|BLOOMFIELDS H|BLOOMFIELS H|BLOOMFILED H|BLOOMFLELD H|BLOOMFTELD H", 'BLOOMFIELD HOMES'),
    (r"NEAL COM|NEAL CM", 'NEAL COMMUNITIES OF SOUTHWEST FLORIDA'),
    (r"^TRUE H(O)?M", 'TRUE HOMES'),
    (r"CHESMAR H|CHESMARH|CHESMAR-H|CHESMAR II|CHESMAR JOMES|CHESMAR T|CHESMER H", 'CHESMAR HOMES'),
    (r"FIRST T.*X.* H", 'FIRST TEXAS HOMES'),
    (r"^SDH|SMITH DOU", 'SMITH DOUGLAS HOMES'),
    (r"^POP H|CARTER HILL H|WESTOVER H|WESTOVER HOM", 'THE CHALLENGER GROUP'),  # https://www.linkedin.com/company/the-challenger-group-inc/about/
    # PoP Homes, Casina Creek Homes(X), Carter Hill Homes, Westover Homes
    (r"^EPCON|EPCOM", 'EPCON COMMUNITIES'),
    (r"HOMES BY WEST.*Y", 'HOMES BY WESTBAY'),
    (r"^CBH H", 'CBH HOMES'),
    (r"^LEGEND H(O)?M", 'LEGEND HOMES'),
    (r"TRUMARK|WATHEN CAST|MANGANO H", 'TRUMARK COMPANIES'),  # the hierarchy is a bit complicated...; Wathen Castanos, Mangano
    (r"EASTWOOD H(O)?M", 'EASTWOOD HOMES'),
    (r"^EDGE H(O)?M", 'EDGE HOMES'),
    (r"ROBSON COMM|^ROBSON H", 'ROBSON COMMUNITIES'),
    (r"^AMERICAN LEGE|AMERICAN LEGAND", 'AMERICAN LEGEND HOMES'),
    # (r"HARKINS BUILDERS", 'HARKINS BUILDERS'),  # no record in data
    (r"^IVORY H(O)?M", 'IVORY HOMES'),
    (r"CASTLEROCK COM", 'CASTLEROCK COMMUNITIES'),
    (r"WESTIN H|WESTIN-H", 'WESTIN HOMES'),
    (r"^STOCK DEV", 'STOCK DEVELOPMENT'),
    (r"^NEW HOME CO |^NEW HOME COM", 'NEW HOME COMPANY'),
    # (r"NexMetro Communities", 'NEXMETRO COMMUNITIES'),  # no record in data
    (r"HABITAT FOR HUM|HABITAT OF HUM", 'HABITAT FOR HUMANITY INTERNATIONAL'),
    (r"^PARK SQ.* H|^PARK SQ.* E", 'PARK SQUARE HOMES'),
    (r"^DAVIDSON H(O)?M", 'DAVIDSON HOMES'),
    (r"BETEN.* H", 'BETENBOUGH HOMES'),
    (r"SCHELL BR", 'SCHELL BROTHERS'),
    (r"LOMBARDO H.*M", 'LOMBARDO COMPANIES'),
    (r"^UNITED H.*M|^GREAT SOUTHERN H|GREAT SOUTHEN H|GREAT SOUTHER H|GREAT SOUTHERM H|GREAT SOUTHERNS H", 'UNITED HOMES GROUP'),  # fka Great Southern Homes
    (r"^GRAND H(O)?M|^GRAND HON", 'GRAND HOMES'),
    (r"DESERT VIEW H|ASPEN VIEW H|HORIZON VIEW H|ARMADILLO H|^VIEW HO", 'VIEW HOMES'),  # https://www.linkedin.com/company/viewhomes/about/
    (r"MCBRIDE .*SO|MCBRDE & SON H|MCBRIDE HOMES ", 'MCBRIDE & SON COMPANIES'),
    (r"SHADDOCK H|SHADDOCK-H", 'SHADDOCK HOMES'),
    (r"KEYSTONE CUSTO", 'KEYSTONE CUSTOM HOMES'),
    (r"HISTORY MA.*", 'HISTORYMAKER HOMES'),
    (r"VAN MET|VAN MATRE", 'VAN METRE COMPANIES'),
    (r"^VIERA B.*D", 'VIERA BUILDERS'),
    (r"TILSON .*H.*M", 'TILSON HOME'),
    (r"SUMMER( )?HILL H(O)?M", 'SUMMERHILL HOMES'),
    # (r"ALLEN EDWIN H", 'ALLEN EDWIN HOMES'),  # no record in data
    (r"^LANDON H", 'LANDON HOMES'),
    (r"^SIGNATURE H(O)?M", 'SIGNATURE HOMES'),
    (r"^WILLIAMS H(O)?M", 'WILLIAMS HOMES'),
    (r"HHHUN|HHHNUT|HHHUT|HHHUUNT", 'HHHUNT'),
    (r"NEWMARK H", 'NEWMARK HOMES'),
    (r"GALAXY B", 'GALAXY BUILDERS'),
    (r"CAVIN.* & C|CAVIN.* & G|CAVIN.* CA", 'CAVINESS & CATES COMMUNITIES'),
    (r"VISIONARY H(O)?M.* B", 'VISIONARY HOME BUILDERS'),  # NOTE: just to differentiate with the below
    (r"VISIONARY H(O)?M", 'VISIONARY HOMES'),
    (r"WILLIAM RY.* H|WILLIAMS RY.* H", 'WILLIAM RYAN HOMES'),
    (r"JOHN MOUR|JOHN MOUI|JOHN MUI|JOHN MUO|JOHN MUR", 'JOHN MOURIER CONSTRUCTION'),
    (r"HAKES BR", 'HAKES BROTHERS'),
    (r"^RIVERSIDE HO.*B", 'RIVERSIDE HOMEBUILDERS'),
    (r"^ELLIOT H|^ELLIOTT H(O)?M", 'ELLIOTT HOMES'),
    (r"^STONE MARI|^STONE MART|^STONE-MAR", 'STONE MARTIN BUILDERS'),
    # (r"TRADITIONS OF AMERICA", 'TRADITIONS OF AMERICA'),  # no record in data
    (r"MAIN STREET H(O)?M", 'MAIN STREET HOMES'),
    (r"ELI.* PROP.* AM|ELI.* PROP.*/ AM", 'ELITE PROPERTIES OF AMERICA'),
    (r"^EASTBROOK H", 'EASTBROOK HOMES'),
    (r"IMPRESSION H", 'IMPRESSION HOMES'),
    (r"SAN JOAQUIN VALLEY", 'SAN JOAQUIN VALLEY HOMES'),
    # (r"^EYA", 'EYA'),  # no record in data
    (r"STYLEC.* B", 'STYLECRAFT BUILDERS'),
    (r"^FULTON H(O)?M", 'FULTON HOMES'),
    (r"^WPG ", 'WOODBRIDGE PACIFIC GROUP'),
    (r"CARUSO H", 'CARUSO HOMES'),
    # (r"BBL", 'BBL BUILDING COMPANY'), # no record in data
    (r"^SITTERLE H|^SITTERIE H", 'SITTERLE HOMES'),
    (r"OLE SO.* P|OLE SOUTHPR", 'OLE SOUTH PROPERTIES'),
    (r"HOMES BY TABER", 'HOMES BY TABER'),
    (r"^EVERMORE H|EVERMOOR HOMES|AMERICAN SOUTHERN H", 'EVERMORE HOMES'),  # fka American Southern Homes
    # (r"CARUSO H", 'EVERGREENE HOMES'),  # no record in data
    (r"HOMES BY DIC", 'HOMES BY DICKERSON'),
    (r"^LEGACY H.*BU|^LEGACY H.*BL", 'LEGACY HOME BUILDERS'),  # NOTE: just to differentiate with the below
    (r"^LEGACY H(O)?M", 'LEGACY HOMES'),
    (r"BLANDFORD H", 'BLANDFORD HOMES'),
    # (r"TCB", 'THE COMMUNITY BUILDERS'),  # hard to indify unique name
    (r"BEECHWOOD HO", 'BEECHWOOD HOMES'),  # NOTE: but this has too few records
    (r"BERKS H(O)?M", 'BERKS HOMES'),
    (r"REGENT H(O)?M", 'REGENT HOMES'),
    (r"^LANDMARK H(O)?M", 'LANDMARK HOMES'),
    # (r"REGENT H(O)?M", 'HOME CREATIONS'),  # claim to be largest in Oklahoma, but can't find record in data
    (r"TRATON H|TRATON C.*P", 'TRATON HOMES'),
    (r"LANDMARK 24", 'LANDMARK 24 HOMES'),
    (r"^SK B", 'SK BUILDERS'),
    # (r"LECESSE", 'LECESSE DEVELOPMENT'),  # no record in data
    (r"ESPERANZA H(O)?M|ESPERANZA H M", 'ESPERANZA HOMES'),
    (r"SEAGATE H(O)?M", 'SEAGATE HOMES'),
    (r"^ANGLIA H|^ANGLIA-H", 'ANGLIA HOMES'),
    (r"NILSON H|NILSSON H", 'NILSON HOMES'),
    (r"VALOR COM", 'VALOR COMMUNITIES'),
    (r"CRAFTMARK H", 'CRAFTMARK HOMES'),
    (r"HANSON BL|HANSON BU", 'HANSON BUILDERS'),
    (r"GRANITE R.DGE B", 'GRANITE RIDGE BUILDERS'),
    (r"PRATT HOME.*B", 'PRATT HOME BUILDERS'),
    (r"CAPSTONE H(O)?M|CAPSTONE-H|CAPSTONE HI", 'CAPSTONE HOMES'),
    (r"UNITED BUILT H|UNITED BILT H|UNITED-B", 'UNITED BUILT HOMES'),
    (r"^IDEAL HM|^IDEAL HOM", 'IDEAL HOMES & NEIGHBORHOODS'),
    # (r"GRAND OAK", 'GRAND OAK BUILDERS'),  # https://grandoakbuilders.com/builders/
    (r"DON JU.* B|NEW MARK H", 'DON JULIAN BUILDERS AND NEW MARK HOMES-KC'),
    (r"NEW TRADITION H|NEW TRADIDTION H|NEW TRADION H|NEW TRADITIION H", 'NEW TRADITION HOMES'),
    (r"CHARTER HM|CHARTER HOM", 'CHARTER HOMES & NEIGHBORHOODS'),
    # (r"COLINA HOMES", 'COLINA HOMES'),  # no record in data
    (r'''SOUTHERN HOMES OF MI|SOUTHERN HOMES MI|SOUTHERN HOMES OF PA|SOUTHERN HOMES OF PO|
         SOUTHERN HOMES PO|SOUTHERN HOMES PA|SOUTHERN HOMES OF BRO|SOUTHERN HOMES BRO|
         SOUTHERN HOMES OF DAV|SOUTHERN HOMES DAV|SOUTHERN HOMES OF EST|SOUTHERN HOMES EST|
         SOUTHERN HOMES OF FL|SOUTHERN HOMES FL''', 'SOUTHERN HOMES'),  # NOTE: there are too many SOUTHERN HOMES, so play it safe
    (r"FIRST AM.* H", 'FIRST AMERICA HOMES'),  # (a Signorelli Company)
    (r"BOISE HUNTER H", 'BOISE HUNTER HOMES'),
    (r"WINDSOR H(O)?M", 'WINDSOR HOMES'),  # NOTE: this one is based in North Carolina (but there's a same name in Fort Wayne, IN)
    (r"^WINDSOR H.* CU", 'WINDSOR HOMES TEXAS'),
    (r"TIM O.*BRIEN|TIM OBRIAN|TIM ORIEN", "TIM O'BRIEN HOMES"),
    (r"MCKEE H", 'MCKEE HOMES'),
    (r"^REGENCY H(O)?M.*LD", 'REGENCY HOMEBUILDERS'),
    (r"^REGENCY H(O)?M", 'REGENCY HOMES'),  # NOTE: just to differentiate with the above. This one is uncertain... (a lot in data, but few online info)
    (r"PIEDMONT RES", 'PIEDMONT RESIDENTIAL'),
    (r"^RODROCK H", 'RODROCK HOMES'),
    (r"TRESIDIO H|TRESIDIO CORP", 'TRESIDIO HOMES'),
    (r"^KEYSTONE H(O)?M.*LD", 'KEYSTONE HOMEBUILDERS'),  # NOTE: just to differentiate with the below
    (r"^KEYSTONE H(O)?M", 'KEYSTONE GROUP'),  # dba Keystone Homes. NOTE: too many keystone homes...
    (r"^KEYSTONE CONS", 'KEYSTONE CONSTRUCTION LLC'),
    (r"STYLECRAFT H", 'STYLECRAFT HOMES'),
    (r"HUNTER Q|HUNTER-Q", 'HUNTER QUINN HOMES'),
    (r"^CLASSICA H", 'CLASSICA HOMES'),
    (r"^CREATIVE H(O)?M(E)?S", 'CREATIVE HOMES'),  # too many creative homes...
    (r"JW COLLECTION", 'JW COLLECTION'),  # NOTE: luxury. no record in data.
    (r"^NAPOLITANO", 'NAPOLITANO HOMES'),  # not sure, but hard to distinguish so collect all
    (r"^INSIGHT H", 'INSIGHT HOMES'),
    (r"BAESSLER H|BAESS.* RES|BAESS.* CON", 'BAESSLER HOMES'),
    (r"PEACHTREE B.* G|^SILVERSTONE RE", 'PEACHTREE BUILDING GROUP'),  # fka Silverstone Communities
    (r"WARMI.* ASS|WARMI[^ ]* HOM", 'THE WARMINGTON GROUP'),
    (r"BUFFINGTON H[^ ]* OF AR|BUFFINGTON HOMES INC", 'BUFFINGTON HOMES OF ARKANSAS'),  # actually the INC name is not certain
    (r"GEMCR[^ ]* H|GEMCFRAFT HOM|GEMCFAFT HOM|GEMCCRAFT HOM|GEMCARAFT HOM", 'GEMCRAFT HOMES'),
    (r"RIZ COM", 'RIZ COMMUNITIES AND DEVELOPMENT'),
    # (r"AHV", 'AMERICAN HOUSING VENTURES'),  # they say they mainly build for rent.
    (r"^WOODLAND H.* HUN", 'WOODLAND HOMES OF HUNTSVILLE'),  # NOTE: also a lot of woodland homes online, and many in data as well
    (r"PROVIDENCE HOMES INC", 'PROVIDENCE HOMES'),  # there are too many Providence Homes online, so here we pick one that has clear definition and with many records
    (r"^WORMALD DEV", 'WORMALD'),
    (r"^LINCOLN PROP", 'WILLOW BRIDGE PROPERTY COMPANY'),  # fka Lincoln Property Co.
    (r"^MANUEL B", 'MANUEL BUILDERS'),
    (r"BILL B.*Z[^ ]* H", 'BILL BEAZLEY HOMES'),
    (r"AMERICAN CLASSIC H", 'AMERICAN CLASSIC HOMES'),
    (r"^UNIVERSAL H(O)?M(E)?S LLC", 'UNIVERSAL HOMES'),  # there's another INC one, not sure if the same
    (r"MCGUINN HO", 'MCGUINN HYBRID HOMES'),
    # (r"BALDWIN & S", 'BALDWIN & SONS'),  # no record in data, but seems to be a big company
    (r"STEPPI[^ ]*STONE|STEPPI[^ ]* STONE", 'STEPPING STONE HOMES'),
    (r"^W[\. ]*B[\. ]* HOME|W.B.HOMES", 'W.B. HOMES'),
    (r"EDWARDS H[^ ]* .*PAS|EDWARDS HMS OF NEW MEXICO LLC", 'EDWARDS HOMES OF EL PASO'),
    (r"EDWARDS H(O)?M", 'EDWARDS HOMES'),  # NOTE: just to collect the rest of edwards homes
    (r"^MILLER[ ]*&[ ]*SM|^MILLER AND SM", 'MILLER & SMITH'),
    (r"^PYAT[^ ]* B", 'PYATT BUILDERS'),
    (r"^MARRANO.*MARC", 'MARRANO HOMES'),
    (r"^ENCE", 'ENCE HOMES'),
    (r"^TIM LEWIS", 'TIM LEWIS COMMUNITIES'),
    (r"^GENTRY H(O)?M", 'GENTRY HOMES'),
    (r"^SUMEER H", 'SUMEER HOMES'),
    (r"^JAMES E[^ ]* CU|JAMES ENGLE H", 'JAMES ENGLE CUSTOM HOMES'),
    (r"^PACIFIC COM[^ ]* B", 'PACIFIC COMMUNITIES BUILDER'),
    (r"FLINTROCK", 'FLINTROCK BUILDERS'),
    (r"^ROBERTSON H", 'ROBERTSON HOMES'),
    (r"\bOLSON HOME(S)?\b(?!.*\bIM\w*\b)", 'THE OLSON COMPANY'),
    (r"MCCAF[^ ]* DEV|MCCAF[^ ]* G", 'MCCAFFREY HOMES'),
    (r"^HAMLET H(O)?M", 'HAMLET HOMES'),
    (r"^ROBUCK H(O)?M", 'ROBUCK HOMES'),
    (r"^TROPICAN", 'TROPICANA HOMES'),
    (r"^RIVERWOOD HOM", 'RIVERWOOD HOMES'),
    (r"^MAGNOLIA H(O)?M", 'MAGNOLIA HOMES'),
    (r"^TWILIGHT H(O)?M", 'TWILIGHT HOMES'),
    (r"^IVEY H(O)?M|^IVEY RE", 'IVEY HOMES'),
    (r"MASTERC[^ ]* B[^ ]* G", 'MASTERCRAFT BUILDER GROUP'),
    (r"^NEWCASTLE H(O)?M", 'NEWCASTLE HOMES'),
    (r"^VANTAGE H(O)?M", 'VANTAGE HOMES'),
    (r"^HOLLAND H(O)?M", 'HOLLAND HOMES'),
    (r"JP BROOKS", 'JP BROOKS BUILDERS'),
    (r"^FORIN[^ ]*", 'FORINO COMPANY'),
    # (r"REYNOLDS CONST|REYNOLDS HOMES|REYNOLDS DEV", 'REYNOLDS COMPANIES'),  # not sure which, so ignore
    (r"^PALO VERDE H(O)?M", 'PALO VERDE HOMES'),
    (r"^LOWDE[^ ]* NEW H", 'LOWDER NEW HOMES'),
    (r"^EDWARD ROSE", 'EDWARD ROSE BUILDING ENTERPRISE'),
    (r"^COVINGTON H(O)?M", 'COVINGTON HOMES'),
    (r"^SILVERTH[^ ]* H(O)?M|^SILVERTH[^ ]* DEV", 'SILVERTHORNE HOMEBUILDERS'),
    (r"^KENDALL H(O)?M", 'KENDALL HOMES'),
    (r"^AWK|KENT HOMES & ASSOC", 'AWK'),  # dba Kent Homes & Associates
    (r"^FRENCH BROT", 'FRENCH BROTHERS'),
    (r"^WINDSONG PR", 'WINDSONG PROPERTIES'),
    (r"WARD COMMU", 'WARD COMMUNITIES'),
    # (r"^INB", 'INB HOMES'),  # should be a large new home builder, but no record in data
    (r"^MCKINNEY B|^MCKINNEY & SON", 'MCKINNEY BUILDERS'),
    (r"^CLEARVIEW H(O)?M", 'CLEARVIEW HOMES'),
    (r"^KEY LAN(.)( )?H", 'KEY LAND HOMES'),
    (r"^GOODWYN BU", 'GOODWYN BUILDING COMPANY'),
    (r"^VENTURA H(O)?M", 'VENTURA HOMES'),
    (r"^THRIVE HOME B|^GREENTREE H|NEW TOWN B[^ ]*L", 'THRIVE HOME BUILDERS'),  # https://www.linkedin.com/company/thrive-home-builders/about/
    (r"^MCARTHUR H(O)?M", 'MCARTHUR HOMES'),
    (r"^SANDCASTLE H(O)?M", 'SANDCASTLE HOMES'),
    (r"^SUNRIVER .*DEV", 'SUNRIVER ST GEORGE DEVELOPMENT'),
    (r"THE BUILDERS G|TBG DEV", 'THE BUILDERS GROUP'),  # actually not so sure
    # (r"SEKISUI", 'SEKISUI HOUSE'),  # Sekisui House, large in Japan, owns MDC Holdings, but no record in data
    (r"RAU[^ ]*[ |-]*COL", 'RAUSCH COLEMAN HOMES'),
    (r"^DRB GR", 'DRB GROUP'),
    (r"^DSL.(.)? H", 'DSLD HOMES'),
    (r"^ADAMS H(?!OLD|OR)|^ADAM H(O)?M[^ ]* .*FL", 'ADAMS HOMES'),
    (r"^THE VILLAGES .*LA", 'THE VILLAGES OF LAKE SUMTER'),
    (r"^MARON[^ ]+( |-)?H(.)?M", 'MARONDA HOMES'),
    (r"^AMERICAN H(.)?M", 'AMH'),  # American Homes 4 Rent
    (r"^LANDSEA", 'LANDSEA HOMES'),
    (r"^HOLIDAY B[^ ]*[L|D]", 'HOLIDAY BUILDERS'),
    (r"^KOLTER", 'THE KOLTER GROUP'),
    (r"^HAYDEN H", 'HAYDEN HOMES'),
    # (r"EMPIRE COMM", 'EMPIRE COMMUNITIES'),  # There are a lot of Empire related names...
    (r"^LONG LAKE$|LONG LAKE(S)? LTD|LONG LAKE(S)? LLC", 'LONG LAKE LTD'),
    (r"^PROMINENCE H", 'PROMINENCE HOMES'),
    (r"^SARATOGA H(.)?M", 'SARATOGA HOMES'),
    (r"^PACESETTER H", 'PACESETTER HOMES'),
    (r"SCHUB[^ ]* MI", 'SCHUBER MITCHELL HOMES'),
    (r"^MEGATEL H[^ ]*M", 'MEGATEL HOMES'),
    (r"ON TOP OF THE WORLD", 'ON TOP OF THE WORLD'),  # no idea how to search for this...
    (r"^ICI ", 'ICI HOMES'),
    (r"THE NEW HOME", 'THE NEW HOME COMPANY'),
    # (r"CHRISTOPHER ALAN H", 'CHRISTOPHER ALAN HOMES'),  # no idea how to search for this...
    (r"^BALL H(.)?M", 'BALL HOMES'),
    (r"^OLT[^ ]*F[ |-]?H", 'OLTHOF HOMES'),
    (r"^CHESAPEAKE H(.)?M", 'CHESAPEAKE HOMES'),
    # (r"ROCKHAVEN H", 'ROCKHAVEN HOMES'),  # no record in data
    (r"BILL CLA(?![^ ]*YTON)[^ ]*( )?H", 'BILL CLARK HOMES'),
    (r"SCOTT FE[^ ]*", 'SCOTT FELDER HOMES'),
    (r"MILES.* C(.)?M[^ ]*( |-)?[B|M]|^MILEST[^ ]* B[^ ]*R", 'MILESTONE COMMUNITY BUILDERS'),
    (r"^MCKINLEY H(.)[M|N]", 'MCKINLEY HOMES'),
    (r"TOUCHSTONE LIVING", 'TOUCHSTONE LIVING'),
    (r"^LIBERTY C(O)?M", 'LIBERTY COMMUNITIES'),
    (r"^CC H(O)?M", 'CC HOMES'),
    (r"^LOGAN H(O)?M", 'LOGAN HOMES'),
    # (r"BRIDGE TOWER", 'BRIDGE TOWER HOMES'),  # no record in data
    (r"^HUBBLE H(.)?M", 'HUBBLE HOMES'),
    (r"^LOKAL ..(.)? |LOKAL COM|LOKAL H(.)M", 'LOKAL HOMES'),
    (r"^A(\.)? SYD", 'A. SYDES CONSTRUCTION'),
    (r"^NATIONAL H(.)?M(.)? CORP", 'NATIONAL HOME CORPORATION'),
    (r"^CITY VEN", 'CITY VENTURES'),
    (r"^BRITE H(.)?M", 'BRITE HOMES'),
    (r"^OUR COUNTRY H", 'OUR COUNTRY HOMES'),
    (r"^GREENLAND H", 'GREENLAND HOMES'),
    (r"^RELIANT H", 'RELIANT HOMES'),
    (r"^GARMAN H", 'GARMAN HOMES'),
    (r"^PARTNERS IN B", 'PARTNERS IN BUILDING'),
    (r"^LIBERTY H(.)?M[^ ]*( )?B", 'LIBERTY HOME BUILDERS'),
    (r"^ANGLE H", 'ANGLE HOMES'),
    (r"^MHC (P|O)F G|^MHC GEORGIA", 'MY HOME COMMUNITIES'),
    (r"^MAIN[V|Y]U", 'MAINVUE HOMES'),
    (r"^SIMMONS H(.)?[M|N]", 'SIMMONS HOMES'),
    (r"DISCOVERY H(.)?M", 'DISCOVERY HOMES'),
    (r"BONAD[A|E]L", 'BONADELLE NEIGHBORHOODS'),
    (r"^GREENSTONE", 'GREENSTONE HOMES'),
    # (r"BEN STOUT CON", 'BEN STOUT CONSTRUCTION'),  # no record in data
    (r"SANDLIN H", 'SANDLIN HOMES'),
    (r"^FRONTIER COM", 'FRONTIER COMMUNITIES'),  # have const, builders, custom homes, dev...
    (r"^MEDALLION H", 'MEDALLION HOMES'),
    (r"^TURNER H(.)?M", 'TURNER HOMES'),
# ------------- above mainly from Top builders list ----------- #
    (r"IMPRESSIONIST HM|IMPRESSIONIST HOM", 'IMPRESSIONIST HOMES'),
    (r"CARUSO B", 'CARUSO BUILDERS'),
    (r"^UNITED BLDR|^UNITED BUILDER", 'UNITED BUILDERS'),
    (r"SOUTHERN HOME BL|SOUTHERN HOME BU", 'SOUTHERN HOME BUILDERS'),
    (r"SOUTHERN HOMEBU", 'SOUTHERN HOMEBUILDERS'),
    (r"^WINDSOR INV", 'WINDSOR INVESTMENTS'),
    (r"TRESGER CONST", 'TRESGER CONSTRUCTION'),
    (r"^JOHN WIE|^JOHN WEI|^JOHN WEE|^JOHN WELA|^JOHN WIC|^JOHN WID", 'JOHN WIELAND HOMES'),  # belongs to PULTEGROUP
    (r"INSIGHT B.*LD.*G", 'INSIGHT BUILDING COMPANY'),
    (r"INSIGHT BU.*RS", 'INSIGHT BUILDERS'),
    (r"^PEACHTREE CM|^PEACHTREE COM", 'PEACHTREE COMMUNITIES'),
    (r"^PEACHTREE CONST", 'PEACHTREE CONSTRUCTION'),
    (r"^PEACHTREE HM|^PEACHTREE HOM", 'PEACHTREE HOMES'),
    (r"^PEACHTREE RES", 'PEACHTREE RESIDENTIAL PROPERTIES'),
    (r"^PEACHTREE T.* COM|^PEACHTREE T.* CM", 'PEACHTREE TOWNHOME COMMUNITIES'),
    (r"BUFFINGTON H[^ ]* .*SAN|BUFFINGTON H[^ ]*.SAN", 'BUFFINGTON HOMES OF SAN ANTONIO'),
    (r"^PROVIDENCE H.*REG", 'PROVIDENCE HOMES OF REGENCY'),
    (r"^WORMALD H", 'WORMALD HOMES'),
    (r"^WORMALD C", 'WORMALD CONDOMINIUM'),
    (r"AMERICAN CLASSIC B", 'AMERICAN CLASSIC BUILDERS'),
    (r"^UNIVERSAL H[^ ]* B", 'UNIVERSAL HOMEBUILDERS'),
    (r"BALDWIN BUI", 'BALDWIN BUILDERS'),
    (r"^CHESAPEAKE HOLDINGS", 'CHESAPEAKE HOLDINGS'),
    (r"^GARMAN B", 'GARMAN BUILDERS'),
    (r"^LIBERTY H[^ ]*R( )?N", 'LIBERTY HARBOR NORTH'),
    (r"^WILS[^ ]*( OF)? HEA", 'WILSHIRE OF HEARTHSTONE VENTURE'),
    (r"^BALLANTRY P", 'BALLANTRY PMC'),
    (r"^JASPER DEV", 'JASPER DEVELOPMENT CORPORATION'),
    (r"TB HOMES LAND", 'TB HOMES LAND'),
    (r"BELLA VISTA C( )?M|BELLA VISTA H(.)?M", 'LEGEND HOMES BY CAMILLO'),  # fka BELLA VISTA HOMES
    (r"MOUNTAIN GATE DEV", 'MOUNTAIN GATE DEVELOPMENT'),
    (r"MOUNTAIN GATE .*PALM", 'MOUNTAIN GATE OF PALM SPRINGS'),
    (r"^BOWEN B[UV]?(I)?(L)?D|BOWEN BUILERS", 'BOWEN BUILDERS GROUP'),
    (r"BOWEN (& |AND |/)?BOW", 'BOWEN & BOWEN CONSTRUCTION'),
    (r"^BOWEN \bFA[^ ]*\b .*|BOWEN FMAILY", 'BOWEN FAMILY HOMES'),
    (r"^HERITAGE H(.)?M(E)?(S)? OF", 'HERITAGE HOMES'),  # only take the OF... ones, cuz there are too many
    (r"^HERITAGE H(.)?M(E)?(S)? & DEV", 'HERITAGE HOMES & DEVELOPMENT'),
    (r"^HERITAGE H(.)?M(E)?( )?B[^ ]*D[^ ]*R", 'HERITAGE HOMEBUILDERS'),
    (r"^RHC CON", 'RHC CONSTRUCTION & REALTY'),
    (r"^HOMESTEAD MUL", 'HOMESTEAD MULTI FAMILY DEVELOPMENT'),
    (r"^TW LEWIS", 'TW LEWIS'),
    (r"^TW OF B", 'TW OF BEACH RESIDENCES'),  # cannot find the exact firm...
]