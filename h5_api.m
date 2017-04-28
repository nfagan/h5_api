classdef h5_api < handle
  
  properties
    h5_file = NaN;
    CHUNK_SIZE = [1e3 1e2 1e2];
    COMPRESSION_LEVEL = 4;
  end
  
  methods
    
    function obj = h5_api(filename)
      
      %   DSP_H5 -- Instantiate an interface to a .h5 database file.
      %
      %     An error is thrown if the given database file is not found.
      %
      %     IN:
      %       - `filename` (char) -- .h5 file to connect to.
      
      if ( nargin == 0 ), return; end;
      obj.assert__file_exists( filename, 'the .h5 file' );
      obj.h5_file = filename;
    end
    
    function create_group(obj, group_path)
      
      %   CREATE_GROUP -- Create a new group in the current .h5 file.
      %
      %     Paths should begin with '/'. Nested paths will be created
      %     automatically. It is an error to create a group that already
      %     exists.
      %
      %     io.create_group( '/Signals' );
      %     io.create_group( '/Signals/NonCommonAveraged' );
      %
      %     IN:
      %       - `group_path` (char)
      
      obj.assert__h5_is_defined();
      obj.assert__file_exists( obj.h5_file, 'the .h5 file' );
      
      group_path = obj.path_components( group_path );
      for i = 1:numel(group_path)
        current = strjoin( group_path(1:i), '/' );
        if ( i == numel(group_path) )
          obj.assert__is_not_group( current );
        end
        if ( obj.is_group( current ) ), continue; end;
        obj.create_group_( current );
      end
    end
    
    function create_group_(obj, one_group)
      
      %   CREATE_GROUP_ -- Private function for creating a group in the
      %     current .h5 file.
      %
      %     IN:
      %       - `one_group` (char) -- Current full path of the group to
      %         create.
      
      plist = 'H5P_DEFAULT';
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', plist );
      gid = H5G.create( fid, one_group, plist, plist, plist );
      H5G.close( gid );
      H5F.close( fid );
    end
    
    function create(obj, filename)
      
      %   CREATE -- Create a new .h5 file.
      %
      %     IN:
      %       - `filename` (char) -- Filename to create.
      
      exists = obj.file_exists( filename );
      assert( ~exists, 'The .h5 file ''%s'' already exists.', filename );
      H5F.create( filename );
      if ( isnan(obj.h5_file) ), obj.h5_file = filename; end
    end
    
    function create_set(obj, spath, sz, chunk, varargin)
      
      %   CREATE_SET -- Create a numeric dataset.
      %
      %     IN:
      %       - `spath` (char) -- Path to the dataset to create.
      %       - `sz` (double) -- Size of the dataset. If any elements are
      %         Inf, the dataset is extensible in those dimensions.
      %       - `chunk` (double) |OPTIONAL| -- Optionally specify the
      %         chunk-size of the dataset, if the set is to be extensible.
      
      spath = obj.ensure_leading_backslash( spath );
      obj.assert__is_not_set( spath );
      if ( nargin < 4 ), chunk = []; end;
      if ( ~any(isinf(sz)) )
        %   create non-extensible.
        h5create( obj.h5_file, spath, sz, 'deflate', obj.COMPRESSION_LEVEL ...
          , varargin{:} );
        obj.writeatt( spath, 'next_row', -1 );
        return;
      end
      if ( isempty(chunk) )
        chunk = obj.get_chunk_( sz ); 
      else
        assert( numel(chunk) == numel(sz), ['If specifying a chunk' ...
          , ' size, the number of elements must match the number of' ...
          , ' elements in the dataset size vector.'] );
      end
      h5create( obj.h5_file, spath, sz, 'ChunkSize', chunk ...
        , 'deflate', obj.COMPRESSION_LEVEL, varargin{:} );
      obj.writeatt( spath, 'next_row', 0 );
    end
    
    function write(obj, data, sgpath)
      
      %   WRITE -- Write data to a dataset or multiple datasets, depending
      %     on the type of data.
      %
      %     If the dataset(s) already exist, they will be overwritten.
      %     Otherwise, they will be created as extensible in the first
      %     dimension.
      %
      %     obj.write( container, group_path ) writes the data and labels
      %     of a Container object to datasets '/data' and '/labels' in the
      %     given group path, marking that group as housing a Container
      %     object.
      %
      %     obj.write( data, set_path ) writes the data of a numeric matrix
      %     to 
      
      if ( isa(data, 'Container') )
        obj.write_container( data, sgpath );
      elseif ( isnumeric(data) )
        obj.write_matrix( data, sgpath );
      else
        error( 'Unsupported data type ''%s''', class(data) );
      end
    end
    
    function write_container(obj, container, gname)
      
      %   WRITE_CONTAINER -- Write a Container object to datasets in a
      %     given group.
      %
      %     If the datasets /data and /labels in the given group already
      %     exist, they will be unlinked (deleted).
      %
      %     IN:
      %       - `container` (Container) -- Container object to save.
      %       - `gname` (char) -- Path to the group in which to save.
      
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      
      data_set_path = [ gname, '/data' ];
      label_set_path = [ gname, '/labels' ];
      
      if ( obj.is_set(data_set_path) ), obj.unlink( data_set_path ); end
      if ( obj.is_set(label_set_path) ), obj.unlink( label_set_path ); end
      
      obj.write_container_( container, gname, 1 );
    end
    
    function write_container_(obj, container, gname, start)
      
      %   WRITE_CONTAINER_ -- Private function for writing a Container
      %     object to an extensible dataset, starting from a given row.
      
      data_set_path = [ gname, '/data' ];
      label_set_path = [ gname, '/labels' ];
      
      if ( ~isa(container.labels, 'SparseLabels') )
        container = container.sparse();
      end
      
      data = container.data;
      labels = container.labels;
      indices = uint8( full(labels.indices) );
      categories = labels.categories;
      labs = labels.labels;
      
      if ( ~obj.is_set(data_set_path) )
        obj.write_matrix_( data, data_set_path, 1 );
        obj.write_matrix_( indices, label_set_path, 1, [Inf, Inf], [] ...
          , 'FillValue', 0 );
      else
        [indices, labs, categories] = ...
          obj.match_( label_set_path, indices, labs, categories );
        obj.write_matrix_( data, data_set_path, start );
        obj.write_matrix_( indices, label_set_path, start );
      end
      
      categories = json( 'encode', categories );
      labs = json( 'encode', labs );
      
      obj.writeatt( label_set_path, 'categories', categories );
      obj.writeatt( label_set_path, 'labels', labs );
      obj.writeatt( data_set_path, 'class', class(container) );
      obj.writeatt( gname, 'class', class(container) );
    end
    
    function write_matrix(obj, mat, spath)
      
      %   WRITE_MATRIX -- Write a numeric matrix to a dataset, erasing the
      %     current contents.
      %
      %     IN:
      %       - `mat` (double, uint8) -- Data matrix to save.
      %       - `spath` (char) -- Path to the dataset in which to save. The
      %         set will be created if it does not exist.
      
      spath = obj.ensure_leading_backslash( spath );
      if ( obj.is_set(spath) && obj.readatt(spath, 'next_row') > 0 )
        obj.unlink( spath );
      end
      obj.write_matrix_( mat, spath, 1 );
    end
    
    function write_matrix_(obj, data, spath, start, varargin)
      
      %   WRITE_MATRIX_ -- Private function for writing numeric data to a
      %     dataset at a given index.
      %
      %     The dataset will be created if it does not already exist.
      %
      %     IN:
      %       - `data` (double) -- Matrix of data to write.
      %       - `spath` (char) -- Path to the dataset.
      %       - `start` (double) |SCALAR| -- Number specifying the row at
      %         which to start writing data.
      %       - `varargin` (cell) -- Additional inputs to optionally pass
      %         to `create_set()` when createing a new set.
      
      if ( ~obj.is_set(spath) )
        data_sz = size( data );
        if ( isempty(varargin) )
          obj.create_set( spath, [Inf, data_sz(2:end)] );
        else
          obj.create_set( spath, varargin{:} );
        end
        next_row = size( data, 1 ) + 1;
      else next_row = start + size(data, 1);
      end
      [start, count] = obj.get_start_count_( data, start );
      h5write( obj.h5_file, spath, data, start, count );
      obj.writeatt( spath, 'next_row', next_row );
      obj.writeatt( spath, 'is_numeric', 1 );
    end
    
    function add(obj, data, sgpath)
      
      %   ADD -- Append data to a given dataset or group of datasets.
      %
      %     Datasets will be created if they do not already exist.
      %
      %     IN:
      %       - `data` (Container, double) -- Container object or numeric
      %         matrix to save.
      %       - `gname` (char) -- Group in which to save.
      
      if ( isa(data, 'Container') )
        obj.add_container_( data, sgpath );
      elseif ( isnumeric(data) || islogical(data) )
        obj.add_matrix_( data, sgpath );
      else
        error( 'Unsupported data type ''%s''', class(data) );
      end
    end
    
    function add_container_(obj, container, gname)
      
      %   ADD_CONTAINER_ -- Private function for appending a Container to
      %     an existing group.
      
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      data_set_path = [ gname, '/data' ];
      if ( ~obj.is_set(data_set_path) )
        next_row = 1;
      else next_row = obj.readatt( data_set_path, 'next_row' );
      end
      obj.write_container_( container, gname, next_row );
    end
    
    function add_matrix_(obj, data, spath)
      
      %   ADD_MATRIX_ -- Private function for appending data to an existing
      %     dataset.
      
      spath = obj.ensure_leading_backslash( spath );
      if ( ~obj.is_set(spath) )
        next_row = 1;
      else next_row = obj.readatt( spath, 'next_row' );
      end
      obj.write_matrix_( data, spath, next_row );
    end
    
    %{
        WRITE HELPERS
    %}
    
    function chunk = get_chunk_(obj, sz)
      
      %   GET_CHUNK_ -- Get an appropriate chunk-size for a dataset.
      %
      %     IN:
      %       - `sz` (double) -- Size vector.
      %     OUT:
      %       - `chunk` (double) -- 1xN vector of chunk-values, where each
      %         `chunk`(i) corresponds to each `sz`(i).
      
      dims = numel( sz );
      chunk = obj.CHUNK_SIZE( 1:dims );
      chunk = min( [chunk; sz] );
    end
    
    function [start, count] = get_start_count_(obj, mat, start)
      
      %   GET_START_COUNT_ -- Get appropriate start and count parameter
      %     values given an incoming data matrix.
      %
      %     IN:
      %       - `mat` (double) -- Data matrix
      %       - `start` (double) |SCALAR| -- Number specifying the row at
      %         which to starting writing data.
      
      dims = ndims( mat );
      start = [ start, ones(1, dims-1) ];
      count = size( mat );
    end
    
    function [new_indices, labs, cats] = ...
        match_(obj, label_set_path, indices, labels, categories)
      
      %   MATCH_ -- Private function for rearranging the indices, labels,
      %     and categories of an incoming Container object to match the
      %     format of the already-stored values.
      %
      %     `match_()` reads the categories and labels attributes of the
      %     current labels dataset. Incoming labels, categories, and
      %     indices are rearranged such that, for labels shared between
      %     incoming and current datasets, the incoming indices are
      %     appended to the correct column of the current indices.
      %
      %     IN:
      %       - `label_set_path` (char) -- Path to the labels dataset to
      %         match.
      %       - `indices` (double) -- Array of indices, converted from
      %         sparse logical.
      %       - `labels` (cell array of strings)
      %       - `categories` (cell array of strings)
      %     OUT:
      %       - `new_indices` (double)
      %       - `labs` (cell array of strings)
      %       - `cats` (cell array of strings)
      
      current_cats = json( 'parse', obj.readatt(label_set_path, 'categories') );
      current_labels = json( 'parse', obj.readatt(label_set_path, 'labels') );
      current_cats = current_cats(:);
      current_labels = current_labels(:);
      labels = labels(:);
      categories = categories(:);
      
      assert( isequal(unique(current_cats), unique(categories)) ...
        , ['The categories must match between the stored object and' ...
        , ' the incoming object.'] );

      shared_labels = intersect( current_labels, labels );
      unique_to_incoming = setdiff( labels, current_labels );
      unique_to_current = setdiff( current_labels, labels );
      n_shared = numel( shared_labels );
      n_incoming = numel( unique_to_incoming );
      n_current = numel( unique_to_current );
      n_total = n_shared + n_incoming + n_current;
      new_indices = zeros( size(indices,1), n_total );
      labs = cell( 1, size(new_indices, 2) );
      cats = cell( size(labs) );
      if ( n_shared > 0 )
        current_inds = cellfun( @(x) find(strcmp(current_labels, x)), shared_labels );
        new_inds = cellfun( @(x) find(strcmp(labels, x)), shared_labels );
        new_indices( :, current_inds ) = indices( :, new_inds );
        cats( current_inds ) = categories( new_inds );
        labs( current_inds ) = labels( new_inds );
      end
      if ( n_incoming > 0 )
        incoming_inds = cellfun( @(x) find(strcmp(labels, x)), unique_to_incoming );
        col = n_shared + n_current + 1;
        new_indices( :, col:end ) = indices( :, incoming_inds );
        cats( col:end ) = categories( incoming_inds );
        labs( col:end ) = labels( incoming_inds );
      end
      if ( n_current > 0 )
        current_inds = cellfun( @(x) find(strcmp(current_labels, x)), unique_to_current );
        cats( current_inds ) = current_cats( current_inds );
        labs( current_inds ) = unique_to_current;
      end
    end
    
    %{
        READ
    %}
    
    function data = read(obj, sgpath)
      
      %   READ -- Load a Container object or matrix from the given path.
      %
      %     If the given path is to a group, it must be a group housing a
      %     Container object. Otherwise, the given path must be to an
      %     existing dataset, which will be read in its entirety.
      %
      %     IN:
      %       - `sgpath` (char) -- Path to the group housing /data and
      %         /labels datasets, or to the dataset to read.
      %     OUT:
      %       - `data` (numeric, Container)
      %   
      
      sgpath = obj.ensure_leading_backslash( sgpath );
      if ( obj.is_group(sgpath) )
        data = obj.read_container_( sgpath );
      else
        obj.assert__is_set( sgpath );
        is_num = obj.readatt( sgpath, 'is_numeric' );
        if ( is_num )
          data = obj.read_matrix_( sgpath );
        else
          error( 'Reading non-numeric data is not currently supported.' );
        end
      end
    end
    
    function cont = read_container_(obj, gpath)
      
      %   READ_CONTAINER_ -- Read in a Container object from a given group.
      %
      %     IN:
      %       - `gpath` (char) -- Path to the group to read.
      %     OUT:
      %       - `cont` (Container) -- Loaded Container object.
      
      labels = obj.read_labels_( gpath );
      obj.assert__is_set( [gpath, '/data'] );
      data = h5read( obj.h5_file, [gpath, '/data'] );     
      cont = Container( data, labels );
    end
    
    function data = read_matrix_(obj, spath)
      
      %   READ_MATRIX_ -- Read in a data matrix from a given dataset.
      %
      %     IN:
      %       - `spath` (char) -- Path to the dataset to read.
      %     OUT:
      %       - `cont` (Container) -- Loaded Container object.
      
      obj.assert__is_set( spath );
      data = h5read( obj.h5_file, spath );
    end
    
    function val = readatt(obj, sname, att)
      
      %   READATT -- Read an attribute from a given dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset or group.
      %       - `att` (char) -- Attribute to read.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      val = h5readatt( obj.h5_file, sname, att );
    end
    
    function writeatt(obj, sname, att_name, att)
      
      %   WRITEATT -- Write an attribute to a given dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset or group.
      %       - `att_name` (char) -- Name of the attribute to write to.
      %       - `att` (double, char) -- Value to write.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      h5writeatt( obj.h5_file, sname, att_name, att );
    end
    
    function encodeatt(obj, sname, att_name, att)
      
      %   ENCODEATT -- Write an attribute to a given dataset, after
      %     encoding it to a JSON string.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att_name` (char) -- Name of the attribute to write to.
      %       - `att` (double, char) -- Value to write.
      
      att = json( 'encode', att );
      obj.writeatt( sname, att_name, att );
    end
    
    function att = decodeatt(obj, sname, att)
      
      %   DECODEATT -- Read an attribute from a given dataset, and parse it
      %     into a Matlab variable.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att` (char) -- Attribute to read.
      
      att = obj.readatt( sname, att );
      att = json( 'parse', att );
    end
    
    function att = parseatt(obj, sname, att)
      
      %   PARSEATT -- Alias for `decodeatt()`.
      
      att = obj.decodeatt( sname, att );
    end
    
    function rmatt(obj, sname, att)
      
      %   RMATT -- Remove an attribute from a dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset or group.
      %       - `att` (char) -- Attribute to delete.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      obj.assert__is_attr( sname, att );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR','H5P_DEFAULT' );
      gid = H5G.open( fid, sname );
      H5A.delete( gid, att );
      H5G.close( gid );
      H5F.close( fid );
    end
    
    function labels = read_labels_(obj, gname)
      
      %   READ_LABELS_ -- Read labels from a given group.
      %
      %     IN:
      %       - `gname` (char) -- Path to a group containing /data and
      %         /labels datasets.
      %     OUT:
      %       - `labs` (SparseLabels) -- Constructed SparseLabels object.
      
      gname = [ gname, '/labels' ];
      obj.assert__is_set( gname );
      indices = h5read( obj.h5_file, gname );
      categories = h5readatt( obj.h5_file, gname, 'categories' );
      labs = h5readatt( obj.h5_file, gname, 'labels' );
      
      categories = json( 'parse', categories );
      categories = categories(:);
      labs = json( 'parse', labs );
      labs = labs(:);
      
      labels = SparseLabels();
      labels.labels = labs;
      labels.categories = categories;
      labels.indices = sparse( logical(indices) );      
    end
        
    function unlink(obj, sname)
      
      %   UNLINK -- Delete a group or dataset.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset or group to delete.
      
      obj.assert__is_set_or_group( sname );
      fileattrib( obj.h5_file, '+w' );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      H5L.delete( fid, sname, 'H5P_DEFAULT' );
      H5F.close(fid);
    end
    
    %{
        PROPERTY VALIDATION
    %}
    
    function set.h5_file(obj, filename)
      
      %   SET.H5_FILE -- Update the `h5_file` property.
      %
      %     The incoming .h5_file must be exist.
      
      obj.assert__file_exists( filename, 'the .h5 file' );
      obj.h5_file = filename;
    end
    
    function set.COMPRESSION_LEVEL(obj, val)
      
      %   SET.COMPRESSION_LEVEL -- Update the `COMPRESSION_LEVEL` property.
      %
      %     The incoming COMPRESSION_LEVEL must be a number between 0 and
      %     9.
      
      assert( isscalar(val) && val <= 9 && val >= 0 ...
        , 'Specify compression level as a number between 0 and 9.' );
      obj.COMPRESSION_LEVEL = val;
    end
    
    function tf = is_set(obj, name)
      
      %   IS_SET -- Return whether a given path is to an existing dataset.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the dataset name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        info = h5info( obj.h5_file, name );
        tf = ~isfield( info, 'Datasets' );
      catch
        return;
      end
    end
    
    function tf = is_attr(obj, sname, att)
      
      %   IS_ATTR -- Return whether a given attribute is an attribute in a
      %     given dataset.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset to query.
      %       - `att` (char) -- Attribute to search for.
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( att, 'char', 'the attribute to query' );
      atts = obj.get_attr_names( sname );
      tf = any( strcmp(atts, att) );
    end
    
    function tf = is_group(obj, name)
      
      %   IS_GROUP -- Return whether a given path is to an existing group.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the group name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        info = h5info( obj.h5_file, name );
        tf = isfield( info, 'Datasets' );
      catch
        return;
      end
    end
    
    function assert__is_set(obj, name)
      
      %   ASSERT__IS_SET -- Ensure a dataset exists.
      
      assert( obj.is_set(name), ['The specified dataset ''%s'' does not' ...
        , ' exist.'], name );
    end
    
    function assert__is_group(obj, name)
      
      %   ASSERT__IS_GROUP -- Ensure a group exists.
      
      assert( obj.is_group(name), ['The specified group ''%s'' does not' ...
        , ' exist.'], name );
    end
    
    function assert__is_set_or_group(obj, name)
      
      %   ASSERT__IS_SET_OR_GROUP -- Ensure a given path is a valid path to
      %     a group or dataset.
      
      obj.assert__isa( name, 'char', 'the set or group name' );
      assert( obj.is_group(name) || obj.is_set(name), ['The specified path' ...
        , ' ''%s'' is not a recognized group or dataset.'], name );
    end
    
    function assert__is_not_group(obj, name)
      
      %   ASSERT__IS_NOT_GROUP -- Ensure a given group does not already
      %     exist.
      
      assert( ~obj.is_group(name), 'The specified group ''%s'' already exists.' ...
        , name );
    end
    
    function assert__is_not_set(obj, name)
      
      %   ASSERT__IS_NOT_SET -- Ensure a given set does not already
      %     exist.
      
      assert( ~obj.is_set(name), 'The specified dataset ''%s'' already exists.' ...
        , name );
    end
    
    function assert__is_attr(obj, sname, att)
      
      %   ASSERT__IS_ATTR -- Ensure an attribute exists.
      
      assert( obj.is_attr(sname, att), ['The specified attribute ''%s''' ...
        , ' does not exist.'], att );
    end
    
    function assert__h5_is_defined(obj)
      
      %   ASSERT__H5_IS_DEFINED -- Ensure a .h5 file has been defined.
      %
      %     Note that this function does not and is not intended to ensure
      %     that a defined .h5 file exists.
      
      assert( ~any(isnan(obj.h5_file)), 'No current .h5 file has been defined.' );
    end
    
    %{
        UTIL
    %}
    
    function list(obj, group_or_set)
      
      %   LIST -- Display the contents of part or all of the current .h5
      %     file.
      %
      %     IN:
      %       - `group_or_set` (char) |OPTIONAL| -- Optionally specify the
      %       group or dataset whose contents are to be displayed.
      
      obj.assert__file_exists( obj.h5_file, 'the .h5 file' );
      if ( nargin < 2 )
        h5disp( obj.h5_file ); return;
      end
      obj.assert__is_set_or_group( group_or_set );
      h5disp( obj.h5_file, group_or_set );
    end
    
    function overview(obj, group_or_set, n_tabs)
      
      %   OVERVIEW -- Display the structure of the .h5 file.
      %
      %     For datasets, only the size of the set is shown explicitly;
      %     attribute names are listed, but their values are not displayed.
      %
      %     IN:
      %       - `group_or_set` (char) |OPTIONAL| -- Path to the group
      %         or set to display. Defaults to the root group '/'.
      %       - `n_tabs` (double) |OPTIONAL| -- Sets the number of tabs to
      %         prepend to the displayed string. Defaults to 0.
      
      if ( nargin < 2 ), group_or_set = '/'; n_tabs = 0; end;
      if ( nargin < 3 ), n_tabs = 0; end;
      info = h5info( obj.h5_file, group_or_set );
      groups = info.Groups;
      spc = '     ';
      for i = 1:numel(groups)
        grp = groups(i);
        tabs = repmat( spc, 1, n_tabs );
        tabs = [ '\n' tabs ];
        split = strsplit( grp.Name, '/' );
        to_print = sprintf( 'Group ''%s/''', split{end} );
        fprintf( [tabs, to_print] );
        if ( ~isempty(grp.Attributes) )
          print_attrs( grp.Attributes, [tabs, spc] );
        end
        if ( ~isempty(grp.Datasets) )
          sets = grp.Datasets;
          tabs = [ tabs, spc ];
          for k = 1:numel(sets)
            fprintf( [tabs, 'Dataset ''%s'''], sets(k).Name );
            attrs = sets(k).Attributes;
            print_sz( sets(k).Dataspace.Size, 'Size', [tabs, spc] );
            print_sz( sets(k).Dataspace.MaxSize, 'MaxSize', [tabs, spc] );
            if ( isempty(attrs) ), continue; end;
            print_attrs( attrs, [tabs, spc] );
          end
          fprintf( '\n' );
        end
        if ( ~isempty(grp.Groups) )
          obj.overview( grp.Name, n_tabs+1 );
        end
      end
      function print_attrs( attrs, spc )
        attrs_array = cell( 1, numel(attrs) );
        for j = 1:numel(attrs)
          attrs_array{j} = sprintf( '"%s"', attrs(j).Name );
        end
        attrs_char = strjoin( attrs_array, ', ' );
        fprintf( [spc, 'Attributes: %s'], attrs_char );
      end
      function print_sz( sz, kind, spc )
        sz = arrayfun( @(x) num2str(x), sz, 'un', false );
        sz = [ '[', strjoin(sz, ' '), ']' ];
        fprintf( [spc, '%s: %s'], kind, sz );
      end
    end
    
    function snames = get_set_names(obj, gname)
      
      %   GET_SET_NAMES -- Return an array of dataset names in the given
      %     group.
      %
      %     IN:
      %       - `gname` (char) -- Group to query.
      %     OUT:
      %       - `snames` (cell array of strings, {})
      
      gname = obj.ensure_leading_backslash( gname );
      info = h5info( obj.h5_file, gname );
      snames = {};
      if ( isempty(info.Datasets) ), return; end;
      snames = { info.Datasets(:).Name };
    end
    
    function anames = get_attr_names(obj, sname)
      
      %   GET_ATTR_NAMES -- Return an array of attribute names in the given
      %     dataset.
      %
      %     IN:
      %       - `sname` (char) -- Dataset to query.
      %     OUT:
      %       - `anames` (cell array of strings, {})
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      info = h5info( obj.h5_file, sname );
      anames = {};
      if ( isempty(info.Attributes) ), return; end;
      anames = { info.Attributes(:).Name };
    end
    
    function split = path_components(obj, str)
      
      %   PATH_COMPONENTS -- Separate a valid path string into
      %     sub-components.
      %
      %     IN:
      %       - `str` (char)
      %     OUT:
      %       - `split` (cell array of strings)
      
      obj.assert__isa( str, 'char', 'the path string' );
      split = strsplit( str, '/' );
      split( cellfun(@isempty, split) ) = [];
    end
    
    function str = ensure_leading_backslash(obj, str)
      
      %   ENSURE_LEADING_BACKSLASH -- Ensure a string begins with '/'
      %
      %     IN:
      %       - `str` (char)
      %     OUT:
      %       - `str` (char)
      
      obj.assert__isa( str, 'char', 'the path string' );
      if ( ~isequal(str(1), '/') ), str = [ '/' str ]; end;
    end
  end
  
  
  methods (Static = true)    
    function tf = file_exists(filename)
      
      %   FILE_EXISTS -- True if the given filename is a path to an
      %     existing file.
      
      h5_api.assert__isa( filename, 'char', 'the .h5 filename' );
      tf = exist( filename, 'file') == 2;
    end
    
    function assert__isa(var, kind, var_kind)
      
      %   ASSERT__ISA -- Ensure a variable is of a given type.
      %
      %     IN:
      %       - `var` (/any/) -- Variable to identify.
      %       - `kind` (char) -- Expected class of `var`.
      %       - `var_kind` (char) |OPTIONAL| -- Optionally specify what
      %         kind of variable `var` is. E.g., 'filename'. Defaults to
      %         'input'.
      
      if ( nargin < 3 ), var_kind = 'input'; end;
      assert( isa(var, kind), 'Expected %s to be a ''%s''; was a ''%s''.' ...
        , var_kind, kind, class(var) );
    end
    
    function assert__file_exists(filename, kind)
      
      %   ASSERT__FILE_EXISTS -- Ensure a file exists.
      %
      %     IN:
      %       - `filename` (char)
      %       - `kind` (char) |OPTIONAL| -- Optionally indicate the kind of
      %         file that was expected to exist. Defaults to an empty
      %         string.
      
      if ( nargin < 2 ), kind = ''; end;
      assert( h5_api.file_exists(filename), ['The specified %s file ''%s'' does' ...
        , ' not exist.'], kind, filename );
    end   
  end
  
end

% function cont = read_selected(obj, gname, selectors)
%       
%       obj.assert__is_group( gname );
%       gname = obj.ensure_leading_backslash( gname );
%       labels = obj.read_labels_( gname );
%       
%       ind = labels.where( selectors );
%       numeric = find( ind );
%       assert( ~isempty(numeric), 'No rows matched the specified criteria.' );
%       non_contiguous = [ 1; diff( numeric ) ~= 1 ];
%       non_contiguous = find( non_contiguous );
%       
%       if ( non_contiguous(end) ~= sum(ind) )
%         non_contiguous(end+1) = sum(ind);
%       end
%       
%       data = zeros( sum(ind), 129, 41 );
%       stp = 1;
%       for i = 1:numel(non_contiguous)-1
%         current_inds = numeric( non_contiguous(i) ) : numeric(non_contiguous(i+1)-1);
%         start = current_inds(1);
%         count = numel( current_inds );
%         count = [ count, Inf, Inf ];
%         data( stp:stp+count(1)-1, :, : ) = h5read( obj.h5_file, [gname, '/data'], [start, 1, 1], count );
%         stp = stp + count(1);
%       end      
%       labels = labels.keep( ind );
%       cont = Container( data, labels );
%     end