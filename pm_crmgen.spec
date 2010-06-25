########################################
# Derived definitions
########################################
%define name pm-crmgen
%define version 1.00
%define release 1
%define prefix /usr
%define ORGARCH pm-crmgen
#
#
Summary: Pacemaker crm-file generator
Name: %{name}
Version: %{version}
Release: %{release}
Group: Applications
Source: %{name}-%{version}.tar.gz
License: GPL
BuildRoot: %{_tmppath}/%{name}-%{version}
BuildRequires: make
BuildArch: noarch
Requires: python >= 2.4, python < 3.0
Requires: pacemaker >= 1.0.9

########################################
%description
Generate crm-file from CSV-file.

########################################
%prep
########################################
rm -rf $RPM_BUILD_ROOT
%setup -q -n %{ORGARCH}

########################################
%build
########################################

########################################
%configure
########################################

########################################
%pre
########################################

########################################
%install
########################################
make DESTDIR=$RPM_BUILD_ROOT install

########################################
%clean
########################################
if
	[ -n "${RPM_BUILD_ROOT}"  -a "${RPM_BUILD_ROOT}" != "/" ]
then
	rm -rf $RPM_BUILD_ROOT
fi
rm -rf $RPM_BUILD_DIR/%{name}

########################################
%post
########################################

########################################
%preun
########################################

########################################
%postun
########################################

########################################
%files
########################################
%defattr(-,root,root)
%{prefix}/bin/pm_crmgen
%dir %{prefix}/share/pacemaker/%{name}
%{prefix}/share/pacemaker/%{name}/pm_crmgen.py
%{prefix}/share/pacemaker/%{name}/pm_crmgen_Env_1.00-1.xls
%ghost %{prefix}/share/pacemaker/%{name}/pm_crmgen.pyc
%ghost %{prefix}/share/pacemaker/%{name}/pm_crmgen.pyo
