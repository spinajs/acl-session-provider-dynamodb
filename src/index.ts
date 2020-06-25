import { SessionProvider, ISession, Session } from "@spinajs/acl";
import { IContainer, Autoinject } from "@spinajs/di";
import { Configuration } from "@spinajs/configuration";
import AWS from 'aws-sdk';
import { Logger } from "@spinajs/log";
import { Log } from "@spinajs/log";
import { InvalidOperation } from "@spinajs/exceptions";
import * as fs from "fs";

export class DynamoDBSessionStore extends SessionProvider {

    @Logger()
    protected Log: Log;

    @Autoinject()
    protected Configuration: Configuration;

    protected DynamoDb: AWS.DynamoDB;

    protected TableName: string;

    public async resolveAsync(_: IContainer): Promise<void> {

        const credentials = this.Configuration.get<string>("acl.session.dynamodb.aws_config_file");
        const region = this.Configuration.get<string>("acl.session.dynamodb.region");

        this.TableName = this.Configuration.get<string>("acl.session.dynamodb.table");

        if (!fs.existsSync(credentials)) {
            throw new InvalidOperation("no aws config file");
        }

        AWS.config.update({ region });

        this.DynamoDb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

    }

    public async restoreSession(sessionId: string): Promise<ISession> {

        const params = {
            TableName: this.TableName,
            Key: {
                'session_id': { S: sessionId }
            },
            ProjectionExpression: 'value'
        };

        const result = await new Promise((res, rej) => {

            this.DynamoDb.getItem(params, (err, data)=>{
                if (err) {
                    rej(err);
                    return;
                }

                res(data.Item);
            });
        });

        if (!result) {
            return null;
        }

        const session = new Session({
            SessionId: sessionId
        });


        return session;

    }

    public async deleteSession(sessionId: string): Promise<void> {

        const params = {
            TableName: this.TableName,
            Key: {
                'session_id': { S: sessionId }
            },
        };

        await new Promise((res, rej) => {

            this.DynamoDb.deleteItem(params, (err, _) => {
                if (err) {
                    rej(err);
                    return;
                }
                res();
            });
        });
    }

    public async updateSession(session: ISession): Promise<void> {

        const params = {
            TableName: this.TableName,
            Item: {
                'session_id': { S: session.SessionId },
                'value': { S: JSON.stringify({ Data: session.Data, Expiration: session.Expiration }) }
            },
        };

        await new Promise((res, rej) => {

            this.DynamoDb.putItem(params, (err, _) => {
                if (err) {
                    rej(err);
                    return;
                }
                res();
            });
        });

    }

    public async refreshSession(sessionId: string): Promise<void> {

        const session = await this.restoreSession(sessionId);
        if (session) {
            session.Expiration = this._getExpirationTime();
            await this.updateSession(session);
        }
    }

    protected _getExpirationTime() {
        const expirationDate = new Date();
        expirationDate.setSeconds(expirationDate.getSeconds() + this.Configuration.get(["acl", "session", "expiration"], 10 * 60));
        return expirationDate;
    }

}