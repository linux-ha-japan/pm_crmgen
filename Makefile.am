MAINTAINERCLEANFILES = Makefile.in
crmgendir = @HA_NOARCHDATAHBDIR@/pm-crmgen
crmgen_SCRIPTS = pm_crmgen.py
crmgen_DATA = pm_crmgen_Env_1.00-1.xls

SHAREDIR = $(crmgendir)
EXTRA_DIST = $(crmgen_DATA)

install-data-hook:
	for dir in $(DESTDIR)$(crmgendir) $(DESTDIR)@bindir@;	\
	do test -p $$dir || mkdir -p $$dir;			\
		chown root $$dir; chmod 755 $$dir;		\
	done
	rm -f $(DESTDIR)@bindir@/pm_crmgen && ln -s $(SHAREDIR)/pm_crmgen.py $(DESTDIR)@bindir@/pm_crmgen
