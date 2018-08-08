from pybuilder.core import Author, init, use_plugin

use_plugin("python.core")
use_plugin("python.coverage")
use_plugin("python.distutils")
use_plugin('python.flake8')
use_plugin("python.install_dependencies")
use_plugin("python.unittest")

use_plugin('copy_resources')
use_plugin('filter_resources')

# use_plugin('pypi:pybuilder_header_plugin')

name = "blinkhealth-zendesk-estimates"
version = '0.1.0.dev0'

authors = [Author('Blink Health', 'support@blinkhealth.com')]
license = 'Commercial'
summary = 'blinkhealth Zendesk Estimates'
description = __doc__

default_task = ['clean', 'analyze', 'publish']


@init
def set_properties(project):
    project.depends_on_requirements("requirements.txt")
    project.build_depends_on_requirements("requirements-dev.txt")
    project.set_property('install_dependencies_upgrade', True)

    project.set_property('verbose', True)

    project.set_property('run_unit_tests_propagate_stdout', True)
    project.set_property('run_unit_tests_propagate_stderr', True)

    project.set_property('coverage_threshold_warn', 50)
    project.set_property('coverage_break_build', False)

    project.set_property('copy_resources_target', '$dir_dist')
    project.get_property('copy_resources_glob').extend(['LICENSE.txt'])
    # project.include_file('component_base.common', 'logger_config.json')

    project.get_property('filter_resources_glob').extend(['**/__init__.py'])

    project.set_property('flake8_verbose_output', True)
    project.set_property('flake8_break_build', True)
    project.set_property('flake8_include_test_sources', True)
    project.set_property('flake8_exclude_patterns', "thirdparty")

    # project.set_property('pybuilder_header_plugin_break_build', True)
    # project.set_property('pybuilder_header_plugin_expected_header', open('header.txt').read())
    # project.set_property('pybuilder_header_plugin_exclude_patterns', ['src/main/python/blinkhealth_zendesk/thirdparty'])

    project.set_property('distutils_commands', 'sdist')
