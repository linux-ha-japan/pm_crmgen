#
# Makefile.am for pm_crmgen
#
MAINTAINERCLEANFILES	= Makefile.in

crmgendir		= $(prefix)/share/pacemaker/pm_crmgen
crmgen_SCRIPTS		= pm_crmgen.py
crmgen_DATA		= pm_crmgen_Env_1.00-1.xls
crmgenlinkdir		= /usr/bin

SPEC			= $(PACKAGE_NAME).spec
TARFILE			= $(PACKAGE_NAME)-$(VERSION).tar.gz
EXTRA_DIST		= $(crmgen_SCRIPTS) $(crmgen_DATA) $(SPEC) autogen.sh

install-data-hook:
	@$(NORMAL_INSTALL)
	test -z $(crmgendir) || $(mkdir_p) $(DESTDIR)$(crmgendir)
	$(INSTALL_SCRIPT) $(crmgen_SCRIPTS) $(DESTDIR)$(crmgendir)
	$(INSTALL_DATA) $(crmgen_DATA) $(DESTDIR)$(crmgendir)
	test -z $(crmgenlinkdir) || $(mkdir_p) $(DESTDIR)$(crmgenlinkdir)
	ln -sf $(crmgendir)/$(crmgen_SCRIPTS) $(DESTDIR)$(crmgenlinkdir)/pm_crmgen

uninstall-local:
	rm -rf $(crmgenlinkdir)/pm_crmgen
	rm -rf $(crmgendir)

$(TARFILE):
	$(MAKE) dist

rpm: clean
	rm -f $(TARFILE)
	$(MAKE) $(SPEC) $(TARFILE)
	cp $(TARFILE) `rpm -E "%_sourcedir"`
	rpmbuild -ba --rmsource $(SPEC)

srpm: clean
	rm -f $(TARFILE)
	$(MAKE) $(SPEC) $(TARFILE)
	cp $(TARFILE) `rpm -E "%_sourcedir"`
	rpmbuild --nodeps -bs --rmsource $(SPEC)
