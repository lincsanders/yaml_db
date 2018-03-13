require 'rubygems'
require 'yaml'
require 'active_record'
require 'rails/railtie'
require 'yaml_db/rake_tasks'
require 'yaml_db/version'
require 'yaml_db/serialization_helper'

module YamlDb
  module Helper
    def self.loader
      Load
    end

    def self.dumper
      Dump
    end

    def self.extension
      "yml"
    end
  end


  module Utils
    def self.chunk_records(records)
      yaml = [ records ].to_yaml
      yaml.sub!(/---\s\n|---\n/, '')
      yaml.sub!('- - -', '  - -')
      yaml
    end

  end

  class Dump < SerializationHelper::Dump

    def self.dump_table_contents(io, table)
      puts "Dumping records for table #{table}..."

      column_names = table_column_names(table)

      count = 0
      each_table_page(table) do |records|
        puts "Dumping records #{count} - #{count + records.to_a.length}..."

        rows = SerializationHelper::Utils.unhash_records(records.to_a, column_names)
        # io.write(Utils.chunk_records(rows))
        io.write({ 
          table => { 
            'columns' => table_column_names(table),
            'from' => count,
            'to' => count += records.to_a.length,
            'records' => rows,
          } 
        }.to_yaml)

        count += records.to_a.length
      end
    end
  end

  class Load < SerializationHelper::Load
    def self.load_documents(io, truncate = true)
      YAML.load_stream(io) do |document|
        document.keys.each do |table_name|
          next if document[table_name].nil?

          load_table(table_name, document[table_name], truncate)
        end
      end
    end
  end

  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path('../tasks/yaml_db_tasks.rake',
__FILE__)
    end
  end

end
