/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Dynamic;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Log;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
using YetaWF.Core.Views.Shared;

namespace YetaWF.DataProvider.EF {

    public abstract class EFDataProviderImpl<CONTAINERCLASS, TYPE, KEY> : IDisposable //TODO: IDisposable tracker update
            where CONTAINERCLASS : IDisposable, new()
            where TYPE : class, new()
    {
        protected CONTAINERCLASS DB { get; private set; }

        public EFDataProviderImpl(CONTAINERCLASS db) { DB = db; NeedDispose = false; }
        public EFDataProviderImpl() { DB = new CONTAINERCLASS(); NeedDispose = true; }

        public void Dispose() { Dispose(true); }
        protected virtual void Dispose(bool disposing) { if (NeedDispose) DB.Dispose(); }
        //~EFDataProviderImpl() { Dispose(false); }
        public bool NeedDispose { get; set; }

        protected YetaWFManager Manager { get { return YetaWFManager.Manager; } }
        protected bool HaveManager { get { return YetaWFManager.HaveManager; } }

        protected TYPE GetItem(KEY key, Func<KEY, TYPE> getFirst, bool Raw = false) {
            var rec = getFirst(key);
            if (rec == null) return default(TYPE);
            if (Raw) return rec;
            TYPE data = new TYPE();
            ObjectSupport.CopyData(rec, data);
            return data;
        }
        protected TYPE GetItem(object key, Func<object, TYPE> getFirst, bool Raw = false) {
            var rec = getFirst(key);
            if (rec == null) return default(TYPE);
            if (Raw) return rec;
            TYPE data = new TYPE();
            ObjectSupport.CopyData(rec, data);
            return data;
        }
        public bool AddItem(TYPE data, Action<TYPE> add, Action save) {
            TYPE rec = new TYPE();
            ObjectSupport.CopyData(data, rec);
            try {
                add(rec);
            } catch (Exception exc) {
                Logging.AddErrorLog("AddItem add for {0} failed - {1}", GetType().FullName, exc);
                throw;
            }
            try {
                save();
            } catch (Exception exc) {
                if (exc.InnerException != null && exc.InnerException.InnerException != null) {
                    SqlException sqlExc = exc.InnerException.InnerException as SqlException;
                    if (sqlExc != null && sqlExc.Number == 2601)
                        return false;// duplicate key
                }
                HandleEntityValidationException(exc);
                Logging.AddLog("AddErrorItem save for {0} failed - {1}", GetType().FullName, exc);
                throw;
            }
            return true;
        }

        private static void HandleEntityValidationException(Exception exc) {
            System.Data.Entity.Validation.DbEntityValidationException dbExc = exc as System.Data.Entity.Validation.DbEntityValidationException;
            if (dbExc != null) {
                string msg = "";
                foreach (System.Data.Entity.Validation.DbEntityValidationResult res in dbExc.EntityValidationErrors) {
                    foreach (System.Data.Entity.Validation.DbValidationError err in res.ValidationErrors) {
                        msg += err.ErrorMessage;
                    }
                }
                throw new InternalError(exc.Message + msg);
            }
        }
        public UpdateStatusEnum UpdateItem(KEY originalKey, TYPE data, Func<KEY, TYPE> getFirst, Action save) {
            var rec = getFirst(originalKey);
            if (rec == null)
                return UpdateStatusEnum.RecordDeleted;
            ObjectSupport.CopyData(data, rec);
            try {
                save();
            } catch (Exception exc) {
                if (exc.InnerException != null && exc.InnerException.InnerException != null) {
                    SqlException sqlExc = exc.InnerException.InnerException as SqlException;
                    if (sqlExc != null && sqlExc.Number == 2601)
                        return UpdateStatusEnum.NewKeyExists;// duplicate key
                }
                HandleEntityValidationException(exc);
                Logging.AddLog("UpdateErrorItem for {0} failed - {1}", GetType().FullName, exc);
                throw;
            }
            return UpdateStatusEnum.OK;
        }
        public bool RemoveItem(KEY key, Func<KEY, TYPE> getFirst, Action<TYPE> remove, Action save) {
            var rec = getFirst(key);
            if (rec == null)
                return false;
            remove(rec);
            try {
                save();
            } catch (Exception exc) {
                HandleEntityValidationException(exc);
                throw;
            }
            return true;
        }

        public IQueryable<TYPE> GetItems(IQueryable<TYPE> recs, int skip, int take,
                    List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total,
                    Func<IQueryable<TYPE>, string, IQueryable<TYPE>> sortbyOrder, Func<IQueryable<TYPE>, IQueryable<TYPE>> sortbyDefault) {
            if (sort != null && sort.Any()) {
                string order = string.Join(",", sort.Select(s => s.ToExpression()));
                recs = sortbyOrder(recs, order);
            } else {
                recs = sortbyDefault(recs);
            }
            if (filters != null && filters.Any()) {
                GridHelper.NormalizeFilters(typeof(TYPE), filters);
                List< DataProviderFilterInfo> flatFilters = DataProviderFilterInfo.CollectAllFilters(filters);
                string[] select = filters.Select(s => s.ToExpression(flatFilters)).ToArray();
                object[] parms = (from f in flatFilters select f.Value).ToArray();
                recs = recs.Where(string.Join(" && ", select), parms);
            }
            total = recs.Count();
            if (skip != 0)
                recs = recs.Skip(skip);
            if (take != 0)
                recs = recs.Take(take);
            return recs;
        }


    }
}
