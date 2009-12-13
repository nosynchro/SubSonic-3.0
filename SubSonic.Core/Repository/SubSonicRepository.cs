﻿// 
//   SubSonic - http://subsonicproject.com
// 
//   The contents of this file are subject to the New BSD
//   License (the "License"); you may not use this file
//   except in compliance with the License. You may obtain a copy of
//   the License at http://www.opensource.org/licenses/bsd-license.php
//  
//   Software distributed under the License is distributed on an 
//   "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
//   implied. See the License for the specific language governing
//   rights and limitations under the License.
// 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SubSonic.Extensions;
using SubSonic.DataProviders;
using SubSonic.Query;
using SubSonic.Schema;
using System.Text;

namespace SubSonic.Repository
{
    /// <summary>
    /// A Repository class which wraps the a Database by type
    /// </summary>
    public class SubSonicRepository<T> : IRepository<T> where T : class, new()
    {
        private readonly IQuerySurface _db;

        public SubSonicRepository(IQuerySurface db)
        {
            _db = db;
        }


        #region IRepository<T> Members

        public ITable GetTable()
        {
            ITable tbl = _db.FindTable(typeof(T).Name);
            return tbl;
        }

        /// <summary>
        /// Loads a T object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item">The item.</param>
        /// <param name="column">The column.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public bool Load<T>(T item, string column, object value) where T : class, new()
        {
            var qry = _db.Select.From(GetTable()).Where(column).IsEqualTo(value);
            bool loaded = false;
            using(var rdr = qry.ExecuteReader())
            {
                if(rdr.Read())
                {
                    rdr.Load(item);
                    loaded = true;
                }
                rdr.Dispose();
            }
            return loaded;
        }

        /// <summary>
        /// Loads a T object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item">The item.</param>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public bool Load<T>(T item, Expression<Func<T, bool>> expression) where T : class, new()
        {
            var qry = _db.Select.From(GetTable()).Where(expression);
            bool loaded = false;
            using(var rdr = qry.ExecuteReader())
            {
                if(rdr.Read())
                {
                    rdr.Load(item);
                    loaded = true;
                }
                rdr.Dispose();
            }
            return loaded;
        }

        /// <summary>
        /// Returns all T items 
        /// </summary>
        public IQueryable<T> GetAll()
        {
            return _db.GetQuery<T>();
        }

        /// <summary>
        /// Returns a single record 
        /// </summary>
        public T GetByKey(object key)
        {
            ITable tbl = GetTable();
            return _db.Select.From(tbl)
                .Where(tbl.PrimaryKey.Name).IsEqualTo(key)
                .ExecuteSingle<T>();
        }

        /// <summary>
        /// Returns a server-side Paged List 
        /// </summary>
        public PagedList<T> GetPaged<TKey>(Func<T, TKey> orderBy, int pageIndex, int pageSize)
        {
            return new PagedList<T>(_db.GetQuery<T>().OrderBy(orderBy).AsQueryable(), pageIndex, pageSize);
        }

        /// <summary>
        /// Returns a server-side Paged List 
        /// </summary>
        public PagedList<T> GetPaged(int pageIndex, int pageSize)
        {
            string[] nothing = null;
            return GetPaged(null, nothing, pageIndex, pageSize);
        }

        /// <summary>
        /// Returns a server-side Paged List, filtered
        /// </summary>
        public PagedList<T> GetPaged(Expression<Func<T, bool>> expression, int pageIndex, int pageSize)
        {
            string[] nothing = null;
            return GetPaged(expression, nothing, pageIndex, pageSize);
        }

        /// <summary>
        /// Returns a server-side Paged List, sorted
        /// </summary>
        public PagedList<T> GetPaged(string sortBy, int pageIndex, int pageSize)
        {
            return GetPaged(null, new string[] { sortBy }, pageIndex, pageSize);
        }

        /// <summary>
        /// Returns a server-side Paged List, filtered and sorted
        /// </summary>
        public PagedList<T> GetPaged(Expression<Func<T, bool>> expression, string sortBy, int pageIndex, int pageSize)
        {
            return GetPaged(expression, new string[] { sortBy }, pageIndex, pageSize);
        }

        /// <summary>
        /// Returns a server-side Paged List, filtered  with multple sort
        /// </summary>
        public PagedList<T> GetPaged(Expression<Func<T, bool>> expression, string[] sortBy, int pageIndex, int pageSize)
        {
            // get total count after applying constraints
            var qry = _db.Select.From<T>();
            if(expression != null)
                qry.Constraints = expression.ParseConstraints().ToList();
            int totalCount = qry.GetRecordCount();

            // set paging
            qry = qry.Paged(pageIndex, pageSize);

            // set ordering
            if (sortBy != null)
            {
                // check for unsafe parameters
                Boolean notsafe = true;
                ITable tbl = GetTable();
                foreach(string s in sortBy) {
                    notsafe = true;
                    foreach (IColumn c in tbl.Columns)
                        if (c.Name.ToLowerInvariant() == s.ToLowerInvariant().Replace("asc", string.Empty).Replace("desc", string.Empty).Trim())
                        {
                            notsafe = false;
                            break;
                        }
                    if(notsafe)
                        throw(new Exception("sortby parameter is not found"));
                }

                StringBuilder sb = new StringBuilder();
                bool isFirst = true;
                foreach (string s in sortBy)
                {
                    if (s == null)
                        continue;
                    if (!isFirst)
                        sb.Append(", ");
                    else
                        isFirst = false;
                    sb.Append(s);
                }
                qry.OrderBys.Add(sb.ToString());
            }

            PagedList<T> result = new PagedList<T>(qry.ExecuteTypedList<T>(), totalCount, pageIndex, pageSize);

            return result;
        }

        /// <summary>
        /// Returns an IQueryable  based on the passed-in Expression  Chinook Database
        /// </summary>
        public IList<T> Search(string column, string value)
        {
            if(!value.EndsWith("%"))
                value += "%";
            var qry = _db.Select.From<T>().Where(column).Like(value).OrderAsc(column);
            return qry.ExecuteTypedList<T>();
        }

        /// <summary>
        /// Returns an IQueryable  based on the passed-in Expression  Chinook Database
        /// </summary>
        public IQueryable<T> Find(Expression<Func<T, bool>> expression)
        {
            return GetAll().Where(expression);
        }

                /// <summary>
        /// Adds a T item to the db
        /// </summary>
        public object Add(T item) {
            return Add(item, _db.Provider);
        }


        /// <summary>
        /// Adds a T item to the db
        /// </summary>
        public object Add(T item, IDataProvider provider)
        {
            var query = item.ToInsertQuery(provider).GetCommand();
            object result = null;
            if(query != null)
            {
                if (provider.Client == DataClient.SqlClient)
                {
                    //add in SCOPE_INDENTITY so we can pull back the ID
                    query.CommandSql += "; SELECT SCOPE_IDENTITY() as new_id";
                }

                /** add "using" keywords to dispose IDataReader rdr object after its get out of the scope **/
                using (var rdr = provider.ExecuteReader(query))
                {
                    if (rdr.Read())
                        result = rdr[0];
                    // repopulate primary key column with newly generated ID
                    if (result != null && result != DBNull.Value)
                    {

                        try
                        {
                            var tbl = provider.FindOrCreateTable(typeof(T));
                            var prop = item.GetType().GetProperty(tbl.PrimaryKey.Name);
                            var settable = result.ChangeTypeTo(prop.PropertyType);
                            prop.SetValue(item, settable, null);

                        }
                        catch (Exception x)
                        {
                            //swallow it - I don't like this per se but this is a convenience and we
                            //don't want to throw the whole thing just because we can't auto-set the value
                        }
                    }
                }

            }
            return result;
        }

                /// <summary>
        /// Adds a bunch of T items 
        ///</summary>
        public void Add(IEnumerable<T> items) {
            Add(items, _db.Provider);

        }


        /// <summary>
        /// Adds a bunch of T items 
        ///</summary>
        public void Add(IEnumerable<T> items, IDataProvider provider)
        {
            BatchQuery bQuery = new BatchQuery(provider);
            foreach(T item in items)
            {
                var query = item.ToInsertQuery(provider);
                bQuery.Queue(query);
            }
            bQuery.Execute();
        }

        /// <summary>
        /// Updates the passed-in T 
        /// </summary>
        public int Update(T item)
        {
            return Update(item, _db.Provider);
        }

        /// <summary>
        /// Updates the passed-in T 
        /// </summary>
        public int Update(T item, IDataProvider provider)
        {
            int result = 0;
            var query = item.ToUpdateQuery(provider).GetCommand();
            if (query != null)
                result = provider.ExecuteQuery(query);
            return result;
        }


        /// <summary>
        /// Updates the passed-in T 
        /// </summary>
        public int Update(IEnumerable<T> items){
            return Update(items, _db.Provider);
        }

        /// <summary>
        /// Updates the passed-in T 
        /// </summary>
        public int Update(IEnumerable<T> items, IDataProvider provider)
        {
            BatchQuery bQuery = new BatchQuery(provider);
            int result = 0;

            foreach(T item in items)
            {
                var query = item.ToUpdateQuery(provider);
                bQuery.Queue(query);
            }
            result = bQuery.Execute();
            return result;
        }

                /// <summary>
        /// Deletes the passed-in T items 
        /// </summary>
        public int Delete(IEnumerable<T> items) {

            return Delete(items, _db.Provider);
        }


        /// <summary>
        /// Deletes the passed-in T items 
        /// </summary>
        public int Delete(IEnumerable<T> items, IDataProvider provider)
        {
            BatchQuery bQuery = new BatchQuery(provider);
            int result = 0;

            foreach(T item in items)
            {
                var query = item.ToDeleteQuery(provider);
                bQuery.Queue(query);
            }
            result = bQuery.Execute();
            return result;
        }

        /// <summary>
        /// Deletes the passed-in T item 
        /// </summary>
        public int Delete(T item) {
            return Delete(item, _db.Provider);
        }


        /// <summary>
        /// Deletes the passed-in T item 
        /// </summary>
        public int Delete(T item, IDataProvider provider)
        {
            int result = 0;
            var query = item.ToDeleteQuery(provider).GetCommand();
            if (query != null)
                result = provider.ExecuteQuery(query);
            return result;
        }

                /// <summary>
        /// Deletes the T item  by Primary Key
        /// </summary>
        public int Delete(object key) {
            return Delete(key, _db.Provider);

        }


        /// <summary>
        /// Deletes the T item  by Primary Key
        /// </summary>
        public int Delete(object key, IDataProvider provider)
        {
            ITable tbl = _db.FindTable(typeof(T).Name);
            int result = 0;
            if(tbl != null)
                result = new Delete<T>(provider).Where(tbl.PrimaryKey.Name).IsEqualTo(key).Execute();
            return result;
        }

        /// <summary>
        /// Deletes 0 to n T items from the Database based on the passed-in Expression
        /// </summary>
        public int DeleteMany(Expression<Func<T, bool>> expression) {
            return DeleteMany(expression, _db.Provider);
        }

        /// <summary>
        /// Deletes 0 to n T items from the Database based on the passed-in Expression
        /// </summary>
        public int DeleteMany(Expression<Func<T, bool>> expression, IDataProvider provider)
        {
            var cmd= _db.Delete(expression).GetCommand();
            return provider.ExecuteQuery(cmd);
        }

        #endregion
    }
}