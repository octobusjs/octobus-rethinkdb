import r from 'rethinkdb';
import Joi from 'joi';
import { expect } from 'chai';
import sinon from 'sinon';
import { createEventDispatcher } from 'octobus.js';
import { generateCRUDservices } from '../src/generateCRUDservices';

const userSchema = {
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  role: Joi.string(),
  age: Joi.number()
};

const createDatabaseIfNotExists = (conn) => (
  r.dbList().run(conn).then((databases) => {
    if (!databases.includes('test')) {
      return r.dbCreate('test').run(conn);
    }

    return true;
  })
);

describe('generateCRUDservices', () => {
  let dispatcher;
  let conn;

  before(() => (
    r.connect({}).then((_conn) => {
      conn = _conn;
      return createDatabaseIfNotExists(conn);
    }).error((err) => {
      throw err;
    })
  ));

  beforeEach(() => {
    dispatcher = createEventDispatcher();

    return generateCRUDservices('entity.User', { r, conn, schema: userSchema })
      .then(({ namespace, map }) => {
        dispatcher.subscribeMap(namespace, map);
      });
  });

  afterEach(() => r.table('User').delete().run(conn));

  after(() => (
    conn.close().error((err) => {
      throw err;
    })
  ));

  it('should call the create hooks', () => {
    const before = sinon.spy();
    const after = sinon.spy();
    dispatcher.onBefore('entity.User.create', before);
    dispatcher.onAfter('entity.User.create', after);
    return dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then(() => {
      expect(before).to.have.been.calledOnce();
      expect(after).to.have.been.calledOnce();
    });
  });

  it('should create a new record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((result) => {
      expect(result.id).to.exist();
      expect(result.firstName).to.equal('John');
      expect(result.lastName).to.equal('Doe');
    })
  ));

  it('should create an array of records', () => (
    dispatcher.dispatch('entity.User.create', [{
      firstName: 'John1',
      lastName: 'Doe1'
    }, {
      firstName: 'John2',
      lastName: 'Doe2'
    }, {
      firstName: 'John3',
      lastName: 'Doe3'
    }]).then((results) => {
      expect(results).to.have.lengthOf(3);
      expect(results[0].lastName).to.equal('Doe1');
      expect(results[1].firstName).to.equal('John2');
      expect(results[2].id).to.exist();
    })
  ));

  it('should find an existing record by id', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => {
      dispatcher.dispatch('entity.User.findById', createdUser.id)
        .then((foundUser) => {
          expect(foundUser.id).to.equal(createdUser.id);
          expect(foundUser.firstName).to.equal('John');
          expect(foundUser.lastName).to.equal('Doe');
        });
    })
  ));

  it('should remove an existing record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.remove', createdUser.id).then(() => (
        dispatcher.dispatch('entity.User.findById', createdUser.id)
          .then((result) => {
            expect(result).to.be.null();
          })
      ))
    ))
  ));

  it('should update an existing record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.update', Object.assign({}, createdUser, {
        lastName: 'Donovan'
      })).then((updatedUser) => {
        expect(updatedUser.id).to.equal(createdUser.id);
        expect(updatedUser.firstName).to.equal('John');
        expect(updatedUser.lastName).to.equal('Donovan');
      })
    ))
  ));

  it('should update multiple records');

  it('should find one record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.findOne', {
        firstName: 'John'
      }).then((foundUser) => {
        expect(foundUser.id).to.equal(createdUser.id);
        expect(foundUser.lastName).to.equal('Doe');
      })
    ))
  ));

  it('should find records');
});
